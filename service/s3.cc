/** s3.cc
    Jeremy Barnes, 3 July 2012
    Copyright (c) 2012 Datacratic.  All rights reserved.

    Code to talk to s3.
*/

#include <atomic>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include <boost/iostreams/stream_buffer.hpp>

#include "tinyxml2/tinyxml2.h"

#include "jml/arch/futex.h"
#include "jml/arch/timers.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/exception_ptr.h"
#include "jml/utils/filter_streams.h"
#include "jml/utils/file_functions.h"

#include "soa/types/basic_value_descriptions.h"
#include "soa/types/date.h"
#include "soa/types/url.h"
#include "soa/utils/print_utils.h"
#include "xml_helpers.h"
#include "message_loop.h"
#include "http_client.h"
#include "fs_utils.h"

#include "soa/service/s3.h"

using namespace std;
using namespace Datacratic;


namespace {

std::unique_ptr<tinyxml2::XMLDocument>
makeBodyXml(const S3Api::Response & response)
{
    if (response.code_ != 200)
        throw ML::Exception("invalid http code returned");
    std::unique_ptr<tinyxml2::XMLDocument> result(new tinyxml2::XMLDocument());
    result->Parse(response.body_.c_str());
    return result;
}

string
xmlAsStr(const std::unique_ptr<tinyxml2::XMLDocument> & document)
{
    tinyxml2::XMLPrinter printer;
    document->Print(&printer);
    return printer.CStr();
}

/** Calculate the signature for a given request. */
string
makeSignature(const string & accessKey,
              const S3Api::Request & request,
              const string & dateStr)
{
    string digest
        = AwsApi::getStringToSignV2Multi(request.verb,
                                         request.bucket,
                                         request.resource,
                                         request.subResource,
                                         request.content.contentType,
                                         request.contentMD5,
                                         dateStr, request.headers);

    return AwsApi::signV2(digest, accessKey);
}


std::mutex s3ApiLock;

/* global key infos */
std::shared_ptr<S3Api> globalS3Api;

/* per bucket key infos */
std::unordered_map<string, std::shared_ptr<S3Api>> specificS3Apis;


/****************************************************************************/
/* S3 GLOBALS                                                               */
/****************************************************************************/

struct S3Globals {
    S3Globals()
        : baseRetryDelay(3), numRetries(-1), loop(1, 0, -1)
    {
        if (numRetries == -1) {
            char * numRetriesEnv = getenv("S3_RETRIES");
            if (numRetriesEnv) {
                numRetries = atoi(numRetriesEnv);
            }
            else {
                numRetries = 45;
            }
        }

        loop.start();
    }

    const shared_ptr<HttpClient> & getClient(const string & bucket,
                                             const string & baseHostname
                                             = "s3.amazonaws.com")
    {
        string hostname = bucket;
        if (hostname.size() > 0) {
            hostname += ".";
        }
        hostname += baseHostname;

        unique_lock<mutex> guard(clientsLock);
        auto & client = clients[hostname];
        if (!client) {
            client.reset(new HttpClient("http://" + hostname, 30));
            client->sendExpect100Continue(false);
            loop.addSource("s3-client-" + hostname, client);
        }

        return client;
    }

    int baseRetryDelay;
    int numRetries;
    MessageLoop loop;

private:
    mutex clientsLock;
    map<string, shared_ptr<HttpClient> > clients;
};

static S3Globals &
getS3Globals()
{
    static S3Globals s3Config;
    return s3Config;
}


/****************************************************************************/
/* S3 URL FS HANDLER                                                        */
/****************************************************************************/

struct S3UrlFsHandler : public UrlFsHandler {
    virtual FsObjectInfo getInfo(const Url & url) const
    {
        string bucket = url.host();
        auto api = getS3ApiForBucket(bucket);
        auto bucketPath = S3Api::parseUri(url.original);
        return api->getObjectInfo(bucket, bucketPath.second);
    }

    virtual FsObjectInfo tryGetInfo(const Url & url) const
    {
        string bucket = url.host();
        auto api = getS3ApiForBucket(bucket);
        auto bucketPath = S3Api::parseUri(url.original);
        return api->tryGetObjectInfo(bucket, bucketPath.second);
    }

    virtual void makeDirectory(const Url & url) const
    {
    }

    virtual bool erase(const Url & url, bool throwException) const
    {
        string bucket = url.host();
        auto api = getS3ApiForBucket(bucket);
        auto bucketPath = S3Api::parseUri(url.original);
        if (throwException) {
            api->eraseObject(bucket, "/" + bucketPath.second);
            return true;
        }
        else {
            return api->tryEraseObject(bucket, "/" + bucketPath.second);
        }
    }

    virtual bool forEach(const Url & prefix,
                         const OnUriObject & onObject,
                         const OnUriSubdir & onSubdir,
                         const string & delimiter,
                         const string & startAt) const
    {
        string bucket = prefix.host();
        auto api = getS3ApiForBucket(bucket);

        bool result = true;

        auto onObject2 = [&] (const string & prefix,
                              const string & objectName,
                              const S3Api::ObjectInfo & info,
                              int depth)
            {
                return onObject("s3://" + bucket + "/" + prefix + objectName,
                                info, depth);
            };

        auto onSubdir2 = [&] (const string & prefix,
                              const string & dirName,
                              int depth)
            {
                return onSubdir("s3://" + bucket + "/" + prefix + dirName,
                                depth);
            };

        // Get rid of leading / on prefix
        string prefix2 = string(prefix.path(), 1);

        api->forEachObject(bucket, prefix2, onObject2,
                           onSubdir ? onSubdir2 : S3Api::OnSubdir(),
                           delimiter, 1, startAt);

        return result;
    }
};


/****************************************************************************/
/* S3 DOWNLOADER                                                            */
/****************************************************************************/

size_t getTotalSystemMemory()
{
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
}

struct S3Downloader {
    S3Downloader(const S3Api * api,
                 const string & bucket,
                 const string & resource, // starts with "/", unescaped (buggy)
                 ssize_t startOffset = 0, ssize_t endOffset = -1)
        : api(api),
          bucket(bucket), resource(resource),
          offset(startOffset),
          baseChunkSize(1024*1024), // start with 1MB and ramp up
          closed(false),
          etagChangedException(false),
          readOffset(0),
          readPartOffset(-1),
          currentChunk(0),
          requestedBytes(0),
          currentRq(0),
          activeRqs(0)
    {
        info = api->getObjectInfo(bucket, resource.substr(1));
        if (!info) {
            throw ML::Exception("missing object: " + resource);
        }
        ExcAssert(!info.etag.empty());

        if (endOffset == -1 || endOffset > info.size) {
            endOffset = info.size;
        }
        downloadSize = endOffset - startOffset;

        /* Maximum chunk size is what we can do in 3 seconds, up to 1% of
           system memory. */
        maxChunkSize = api->bandwidthToServiceMbps * 3.0 * 1000000;
        size_t sysMemory = getTotalSystemMemory();
        maxChunkSize = std::min(maxChunkSize, sysMemory / 100);

        /* The maximum number of concurrent requests is set depending on
           the total size of the stream. */
        maxRqs = 1;
        if (info.size > 1024 * 1024)
            maxRqs = 5;
        if (info.size > 16 * 1024 * 1024)
            maxRqs = 15;
        if (info.size > 256 * 1024 * 1024)
            maxRqs = 30;
        chunks.resize(maxRqs);

        /* Hack to ensure that the file's last modified time is earlier than 1
           seconds before the start time of the download. Useful for the etag
           check below. Should seldom be executed in practice. */
        Date now = Date::now();
        if (info.lastModified > now.plusSeconds(-1)) {
            ML::sleep(1);
        }

        /* Kick start the requests */
        ensureRequests();
    }

    ~S3Downloader()
    {
        /* We ensure at runtime that "close" is called because it is mandatory
           for the proper cleanup of active requests. Because "close" can
           throw, we cannot however call it from the destructor. */
        if (!closed) {
            cerr << "destroying S3Downloader without invoking close()\n";
            abort();
        }
    }

    std::streamsize read(char * s, std::streamsize n)
    {
        if (closed) {
            throw ML::Exception("invoking read() on a closed download");
        }

        if (endOfDownload()) {
            return -1;
        }

        if (readPartOffset == -1) {
            waitNextPart();
        }
        ensureRequests();

        size_t toDo = min<size_t>(readPart.size() - readPartOffset,
                                  n);
        const char * start = readPart.c_str() + readPartOffset;
        std::copy(start, start + toDo, s);

        readPartOffset += toDo;
        if (readPartOffset == readPart.size()) {
            readPartOffset = -1;
        }

        readOffset += toDo;

        return toDo;
    }

    uint64_t getDownloadSize()
        const
    {
        return downloadSize;
    }

    bool endOfDownload()
        const
    {
        return (readOffset == downloadSize);
    }

    void close()
    {
        closed = true;
        while (activeRqs > 0) {
            ML::futex_wait(activeRqs, activeRqs);
        }
        if (etagChangedException) {
            handleEtagChange();
        }
        excPtrHandler.rethrowIfSet();
    }

private:
    /* download Chunk */
    struct Chunk {
        enum State {
            IDLE,
            QUERY,
            RESPONSE
        };

        Chunk() noexcept
            : state(IDLE)
        {
        }

        Chunk(Chunk && other) noexcept
            : state(other.state.load()),
              data(std::move(other.data))
        {
        }

        void setQuerying()
        {
            ExcAssertEqual(state, IDLE);
            setState(QUERY);
        }

        void assign(string newData)
        {
            ExcAssertEqual(state, QUERY);
            data = move(newData);
            setState(RESPONSE);
            ML::futex_wake(state);
        }

        string retrieve()
        {
            ExcAssertEqual(state, RESPONSE);
            string chunkData = std::move(data);
            setState(IDLE);
            return std::move(chunkData);
        }

        void setState(int newState)
        {
            state = newState;
            ML::futex_wake(state);
        }

        bool isIdle()
            const
        {
            return (state == IDLE);
        }

        bool waitResponse(double timeout)
            const
        {
            if (timeout > 0.0) {
                int old = state;
                if (state != RESPONSE) {
                    ML::futex_wait(state, old, timeout);
                }
            }

            return (state == RESPONSE);
        }

    private:
        std::atomic<int> state;
        string data;
    };

    void waitNextPart()
    {
        unsigned int chunkNr(currentChunk % maxRqs);
        Chunk & chunk = chunks[chunkNr];
        while (!excPtrHandler.hasException() && !chunk.waitResponse(1.0));
        if (etagChangedException) {
            handleEtagChange();
        }
        excPtrHandler.rethrowIfSet();
        readPart = chunk.retrieve();
        readPartOffset = 0;
        currentChunk++;
    }

    void ensureRequests()
    {
        while (true) {
            if (excPtrHandler.hasException()) {
                break;
            }
            if (activeRqs == maxRqs) {
                break;
            }
            ExcAssert(activeRqs < maxRqs);
            if (requestedBytes == downloadSize) {
                break;
            }
            ExcAssert(requestedBytes < downloadSize);

            Chunk & chunk = chunks[currentRq % maxRqs];
            if (!chunk.isIdle()) {
                break;
            }

            ensureRequest();
        }
    }

    void ensureRequest()
    {
        size_t chunkSize = getChunkSize(currentRq);
        uint64_t end = requestedBytes + chunkSize;
        if (end > info.size) {
            end = info.size;
            chunkSize = end - requestedBytes;
        }

        unsigned int chunkNr = currentRq % maxRqs;
        Chunk & chunk = chunks[chunkNr];
        activeRqs++;
        chunk.setQuerying();

        auto onResponse
            = [&, chunkNr, chunkSize] (S3Api::Response && response,
                                       std::exception_ptr excPtr) {
            this->handleResponse(chunkNr, chunkSize,
                                 std::move(response), excPtr);
        };
        S3Api::Range range(offset + requestedBytes, chunkSize);
        api->getAsync(onResponse, bucket, resource, range);
        ExcAssertLess(currentRq, UINT_MAX);
        currentRq++;
        requestedBytes += chunkSize;
    }

    void handleResponse(unsigned int chunkNr, size_t chunkSize,
                        S3Api::Response && response,
                        std::exception_ptr excPtr)
    {
        try {
            if (excPtr) {
                rethrow_exception(excPtr);
            }

            if (response.code_ != 200 && response.code_ != 206) {
                throw ML::Exception("http error "
                                    + to_string(response.code_)
                                    + " while getting chunk "
                                    + xmlAsStr(makeBodyXml(response)));
            }

            /* It can sometimes happen that a file changes during download i.e
               it is being overwritten. Make sure we check for this condition
               and throw an appropriate exception. */
            auto headers = response.parsedHeaders();
            const string & chunkEtag = headers.at("etag");
            if (chunkEtag != info.etag) {
                etagChangedException = true;
                throw ML::Exception("chunk etag '%s' (size: %lu, hex: '%s')"
                                    " differs from original etag '%s' (size:"
                                    " %lu, hex: '%s') of file '%s'",
                                    chunkEtag.c_str(), chunkEtag.size(),
                                    ML::hexify_string(chunkEtag).c_str(),
                                    info.etag.c_str(), info.etag.size(),
                                    ML::hexify_string(info.etag).c_str(),
                                    resource.c_str());
            }
            if (response.body().size() != chunkSize) {
                throw ML::Exception("chunk sizes differ (%lu, %lu) for '%s'",
                                    response.body().size(), chunkSize,
                                    resource.c_str());
            }
            ExcAssertEqual(response.body().size(), chunkSize);
            Chunk & chunk = chunks[chunkNr];
            chunk.assign(std::move(response.body_));
        }
        catch (const std::exception & exc) {
            excPtrHandler.takeCurrentException();
        }
        activeRqs--;
        ML::futex_wake(activeRqs);
    }

    void handleEtagChange()
    {
        auto newInfo = api->getObjectInfo(bucket, resource.substr(1));
        ExcAssert(newInfo.lastModified >= info.lastModified);
        if (newInfo.lastModified == info.lastModified) {
            cerr << ML::format("etag change: anomalous difference between chunk"
                               " etag and file etag for file '%s', since last"
                               " modification date stayed the same\n",
                               resource.c_str());
        }
    }

    size_t getChunkSize(unsigned int chunkNbr)
        const
    {
        size_t chunkSize = std::min(baseChunkSize * (1 << (chunkNbr / 2)),
                                    maxChunkSize);
        return chunkSize;
    }

    /* static variables, set during or right after construction */
    const S3Api * api;
    string bucket;
    string resource;
    S3Api::ObjectInfo info;
    uint64_t offset; /* the lower position in the file from which the download
                      * is started */
    uint64_t downloadSize; /* total number of bytes to download */
    size_t baseChunkSize;
    size_t maxChunkSize;

    bool closed; /* whether close() was invoked */
    ML::ExceptionPtrHandler excPtrHandler;
    std::atomic<bool> etagChangedException; /* whether the exception above is
                                             * due to a difference between the
                                             * etag of a chunk and the
                                             * original etag of the object */

    /* read thread */
    uint64_t readOffset; /* number of bytes from the entire stream that
                          * have been returned to the caller */
    string readPart; /* data buffer for the part of the stream being
                      * transferred to the caller */
    ssize_t readPartOffset; /* number of bytes from "readPart" that have
                             * been returned to the caller, or -1 when
                             * awaiting a new part */
    unsigned int currentChunk; /* chunk being read */

    /* http requests */
    unsigned int maxRqs; /* maximum number of concurrent http requests */
    uint64_t requestedBytes; /* total number of bytes that have been
                              * requested, including the non-received ones */
    vector<Chunk> chunks; /* chunks */
    unsigned int currentRq;  /* number of done requests */
    atomic<unsigned int> activeRqs; /* number of pending http requests */
};


/****************************************************************************/
/* S3 UPLOADER                                                              */
/****************************************************************************/

inline void touchByte(const char * c)
{
    __asm__(" # [in]":: [in] "r" (*c):);
}

inline void touch(const char * start, size_t size)
{
    const char * current = start - (intptr_t) start % 4096;
    if (current < start) {
        current += 4096;
    }
    const char * end = start + size;
    for (; current < end; current += 4096) {
        touchByte(current);
    }
}


struct S3Uploader {
    S3Uploader(const S3Api * api,
               const string & bucket,
               const string & resource, // starts with "/", unescaped (buggy)
               const ML::OnUriHandlerException & excCallback,
               const S3Api::ObjectMetadata & objectMetadata)
        : api(api),
          bucket(bucket), resource(resource),
          metadata(objectMetadata),
          onException(excCallback),
          closed(false),
          chunkSize(8 * 1024 * 1024), // start with 8MB and ramp up
          currentRq(0),
          activeRqs(0)
    {
        /* Maximum chunk size is what we can do in 3 seconds, up to 1% of
           system memory. */
#if 0
        maxChunkSize = api->bandwidthToServiceMbps * 3.0 * 1000000;
        size_t sysMemory = getTotalSystemMemory();
        maxChunkSize = std::min(maxChunkSize, sysMemory / 100);
#else
        maxChunkSize = 64 * 1024 * 1024;
#endif

        try {
            S3Api::MultiPartUpload upload
              = api->obtainMultiPartUpload(bucket, resource, metadata,
                                           S3Api::UR_EXCLUSIVE);
            uploadId = upload.id;
        }
        catch (...) {
            if (onException) {
                onException();
            }
            throw;
        }
    }

    ~S3Uploader()
    {
        /* We ensure at runtime that "close" is called because it is mandatory
           for the proper cleanup of active requests. Because "close" can
           throw, we cannot however call it from the destructor. */
        if (!closed) {
            cerr << "destroying S3Uploader without invoking close()\n";
            abort();
        }
    }

    std::streamsize write(const char * s, std::streamsize n)
    {
        std::streamsize done(0);

        touch(s, n);

        size_t remaining = chunkSize - current.size();
        while (n > 0) {
            if (excPtrHandler.hasException() && onException) {
                onException();
            }
            excPtrHandler.rethrowIfSet();
            size_t toDo = min(remaining, (size_t) n);
            if (toDo < n) {
                flush();
                remaining = chunkSize - current.size();
            }
            current.append(s, toDo);
            s += toDo;
            n -= toDo;
            done += toDo;
            remaining -= toDo;
        }

        return done;
    }

    void flush(bool force = false)
    {
        if (!force) {
            ExcAssert(current.size() > 0);
        }
        while (activeRqs == metadata.numRequests) {
            ML::futex_wait(activeRqs, activeRqs);
        }
        if (excPtrHandler.hasException() && onException) {
            onException();
        }
        excPtrHandler.rethrowIfSet();

        unsigned int rqNbr(currentRq);
        auto onResponse = [&, rqNbr] (S3Api::Response && response,
                                      std::exception_ptr excPtr) {
            this->handleResponse(rqNbr, std::move(response), excPtr);
        };

        unsigned int partNumber = currentRq + 1;
        if (etags.size() < partNumber) {
            etags.resize(partNumber);
        }

        activeRqs++;
        api->putAsync(onResponse, bucket, resource,
                      ML::format("partNumber=%d&uploadId=%s",
                                 partNumber, uploadId),
                      {}, {}, current, true);

        if (currentRq % 5 == 0 && chunkSize < maxChunkSize)
            chunkSize *= 2;

        current.clear();
        currentRq = partNumber;
    }

    void handleResponse(unsigned int rqNbr,
                        S3Api::Response && response,
                        std::exception_ptr excPtr)
    {
        try {
            if (excPtr) {
                rethrow_exception(excPtr);
            }

            if (response.code_ != 200) {
                cerr << xmlAsStr(makeBodyXml(response)) << endl;
                throw ML::Exception("put didn't work: %d", (int)response.code_);
            }

            auto headers = response.parsedHeaders();
            const string & etag = headers.at("etag");
            ExcAssert(etag.size() > 0);
            etags[rqNbr] = etag;
        }
        catch (const std::exception & exc) {
            excPtrHandler.takeCurrentException();
        }
        activeRqs--;
        ML::futex_wake(activeRqs);
    }

    string close()
    {
        closed = true;
        if (current.size() > 0) {
            flush();
        }
        else if (currentRq == 0) {
            /* for empty files, force the creation of a single empty part */
            flush(true);
        }
        while (activeRqs > 0) {
            ML::futex_wait(activeRqs, activeRqs);
        }
        if (excPtrHandler.hasException() && onException) {
            onException();
        }
        excPtrHandler.rethrowIfSet();

        string finalEtag;
        try {
            finalEtag = api->finishMultiPartUpload(bucket, resource,
                                                   uploadId, etags);
        }
        catch (...) {
            if (onException) {
                onException();
            }
            throw;
        }

        return finalEtag;
    }

private:
    const S3Api * api;
    string bucket;
    string resource;
    S3Api::ObjectMetadata metadata;
    ML::OnUriHandlerException onException;

    size_t maxChunkSize;
    string uploadId;

    /* state variables, used between "start" and "stop" */
    bool closed; /* whether close() was invoked */
    ML::ExceptionPtrHandler excPtrHandler;

    string current; /* current chunk data */
    size_t chunkSize; /* current chunk size */
    vector<string> etags; /* etags of individual chunks */
    unsigned int currentRq;  /* number of done requests */
    atomic<unsigned int> activeRqs; /* number of pending http requests */
};


struct AtInit {
    AtInit() {
        registerUrlFsHandler("s3", new S3UrlFsHandler());
    }
} atInit;


/****************************************************************************/
/* S3 REQUEST STATE                                                         */
/****************************************************************************/

struct S3RequestState {
    S3RequestState(const S3Api & s3Api, S3Api::Request && rq,
                   const S3Api::OnResponse & onResponse)
        : accessKeyId(s3Api.accessKeyId), accessKey(s3Api.accessKey),
          bandwidthToServiceMbps(s3Api.bandwidthToServiceMbps),
          rq(std::move(rq)),
          range(rq.downloadRange),
          onResponse(onResponse),
          retries(0)
    {
    }

    RestParams makeHeaders()
        const
    {
        string date = Date::now().printRfc2616();
        string sig = makeSignature(accessKey, rq, date);
        string auth = "AWS " + accessKeyId + ":" + sig;

        RestParams headers = rq.headers;
        headers.push_back({"Date", date});
        headers.push_back({"Authorization", auth});
        if (rq.contentMD5.size() > 0) {
            headers.push_back({"Content-MD5", rq.contentMD5});
        }
        if (rq.useRange()) {
            headers.push_back({"Range", range.headerValue()});
        }

        return headers;
    }

    int makeTimeout()
        const
    {
        double expectedTimeSeconds
            = (range.size / 1000000.0) / bandwidthToServiceMbps;
        return 15 + std::max<int>(30, expectedTimeSeconds * 6);
    }

    string accessKeyId;
    string accessKey;
    double bandwidthToServiceMbps;

    S3Api::Request rq;
    S3Api::Range range;
    S3Api::OnResponse onResponse;

    string body;
    string requestBody;
    int retries;
};


/****************************************************************************/
/* S3 REQUEST CALLBACKS                                                     */
/****************************************************************************/

struct S3RequestCallbacks : public HttpClientCallbacks {
    S3RequestCallbacks(const shared_ptr<S3RequestState> & state)
        : state_(state)
    {
    }

    virtual void onResponseStart(const HttpRequest & rq,
                                 const string & httpVersion,
                                 int code);
    virtual void onHeader(const HttpRequest & rq,
                          const char * data, size_t size);
    virtual void onData(const HttpRequest & rq,
                        const char * data, size_t size);
    virtual void onDone(const HttpRequest & rq,
                        HttpClientError errorCode);

    pair<string, string> detectXMLError() const;
    string httpErrorContext() const;
    void scheduleRestart() const;

    shared_ptr<S3RequestState> state_;

    S3Api::Response response_;
    string header_;
};

void
performStateRequest(const shared_ptr<S3RequestState> & state)
{
    const auto & client = getS3Globals().getClient(state->rq.bucket);

    const S3Api::Request & request = state->rq;
    string resource = state->rq.makeUrl();
    auto callbacks = make_shared<S3RequestCallbacks>(state);
    RestParams headers = state->makeHeaders();
    int timeout = state->makeTimeout();

    if (!client->enqueueRequest(request.verb, resource,
                                callbacks,
                                request.content,
                                /* query params already encoded in resource */
                                {},
                                headers,
                                timeout)) {
        /* TODO: should invoke onResponse with "too many requests" */
        throw ML::Exception("the http client could not enqueue the request");
    }
}

void
S3RequestCallbacks::
onResponseStart(const HttpRequest & rq, const string & httpVersion,
                int code)
{
    response_.code_ = code;
}

void
S3RequestCallbacks::
onHeader(const HttpRequest & rq, const char * data, size_t size)
{
    header_.append(data, size);
}

void
S3RequestCallbacks::
onData(const HttpRequest & rq, const char * data, size_t size)
{
    state_->requestBody.append(data, size);
}

void
S3RequestCallbacks::
onDone(const HttpRequest & rq, HttpClientError errorCode)
{
    bool errorCondition(false);
    bool recoverable(false);
    string errorCause;
    string errorDetails;

    if (errorCode == HttpClientError::None) {
        auto xmlError = detectXMLError();
        if (!xmlError.first.empty()) {
            errorCondition = true;
            errorCause = "REST error code \"" + xmlError.first + "\"";
            errorDetails = ("http status: "
                            + to_string(response_.code_) + "\n"
                            + "message: " + xmlError.second);

            /* retry on temporary request errors */
            if (xmlError.first == "InternalError"
                || xmlError.first == "RequestTimeout"
                || xmlError.first == "RequestTimeTooSkewed") {
                recoverable = true;
            }
        }
        else if (response_.code_ >= 300 && response_.code_ != 404) {
            errorCondition = true;
            errorCause = "HTTP status code " + to_string(response_.code_);
            errorDetails = httpErrorContext();

            /* retry on 50X range errors */
            if (response_.code_ >= 500 and response_.code_ < 505) {
                recoverable = true;
            }
        }
    }
    else {
        errorCondition = true;
        errorCause = "internal error \"" + errorMessage(errorCode) + "\"";
        recoverable = true;
        if (state_->rq.useRange()) {
            state_->range.adjust(state_->requestBody.size());
            state_->body.append(state_->requestBody);
        }
        else {
            state_->body.clear();
        }
    }

    if (errorCondition) {
        string recoverability;
        if (recoverable) {
            if (state_->retries < getS3Globals().numRetries) {
                recoverability = "The operation will be retried.";
                state_->retries++;
            }
            else {
                recoverability = "The operation was retried too many times.";
                recoverable = false;
            }
        }
        else {
            recoverability = "The error is non recoverable.";
        }

        string message("S3 operation failed with " + errorCause);

        string diagnostic(message + "\n"
                          + "operation: " + state_->rq.verb + " " + rq.url_
                          + "\n");
        if (!errorDetails.empty()) {
            diagnostic += errorDetails + "\n";
        }
        diagnostic += recoverability + "\n";

        cerr << diagnostic;

        header_.clear();
        state_->requestBody.clear();
        if (recoverable) {
            scheduleRestart();
        }
        else {
            auto excPtr = make_exception_ptr(ML::Exception(message));
            state_->onResponse(std::move(response_), excPtr);
        }
    }
    else {
        response_.header_.parse(header_, false);
        header_.clear();
        state_->body.append(state_->requestBody);
        state_->requestBody.clear();
        response_.body_ = std::move(state_->body);
        state_->onResponse(std::move(response_), nullptr);
    }
}

pair<string, string>
S3RequestCallbacks::
detectXMLError()
    const
{
    /* Detect so-called "REST error"
       (http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)

       Some S3 methods may return an XML error AND still have a 200 HTTP
       status code:
       http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
       Explanation of the why:
       https://github.com/aws/aws-sdk-go/issues/501.
    */

    pair<string, string> xmlError; /* {code, message} */

    const S3Api::Request & request = state_->rq;
    if (!(response_.code_ == 200
          && (request.verb == "GET" || request.verb == "HEAD"))
        && ((headers.find("Content-Type: application/xml")
             != string::npos)
            || (headers.find("content-type: application/xml")
                != string::npos))) {
        if (!state_->requestBody.empty()) {
            std::unique_ptr<tinyxml2::XMLDocument> localXml;
            localXml.reset(new tinyxml2::XMLDocument());
            localXml->Parse(state_->requestBody.c_str());
            auto element = tinyxml2::XMLHandle(*localXml)
                .FirstChildElement("Error")
                .ToElement();
            if (element) {
                xmlError.first = extract<string>(element, "Code");
                xmlError.second = extract<string>(element, "Message");
            }
        }
    }

    return xmlError;
}

string
S3RequestCallbacks::
httpErrorContext()
    const
{
    string context = "http status: " + to_string(response_.code_) + "\n";
    if (header_.size() > 0) {
        context += "response headers:\n" + header_;
    }
    if (!state_->requestBody.empty()) {
        context += (string("response body (")
                    + to_string(state_->requestBody.size())
                    + " bytes):\n" + state_->requestBody + "\n");
    }

    return context;
}

void
S3RequestCallbacks::
scheduleRestart()
    const
{
    S3Globals & globals = getS3Globals();

    // allow a maximum of 384 seconds for retry delays (1 << 7 * 3)
    int multiplier = (state_->retries < 8
                      ? (1 << state_->retries)
                      : state_->retries << 7);
    double numSeconds = ::random() % (globals.baseRetryDelay
                                      * multiplier);
    if (numSeconds == 0) {
        numSeconds = globals.baseRetryDelay * multiplier;
    }

    numSeconds = 0.05;

    const S3Api::Request & request = state_->rq;
    cerr << ("S3 operation retry in " + to_string(numSeconds) + " seconds: "
             + request.verb + " " + request.resource + "\n");

    auto timer = make_shared<PeriodicEventSource>();

    auto state = state_;
    auto onTimeout = [&, timer, state] (uint64_t ticks) {
        S3Globals & globals = getS3Globals();
        performStateRequest(state);
        globals.loop.removeSource(timer.get());
    };
    timer->init(numSeconds, std::move(onTimeout));
    globals.loop.addSource("retry-timer-" + randomString(8), timer);
}

}


namespace Datacratic {

/****************************************************************************/
/* S3 CONFIG DESCRIPTION                                                    */
/****************************************************************************/

S3ConfigDescription::
S3ConfigDescription()
{
    addField("accessKeyId", &S3Config::accessKeyId, "");
    addField("accessKey", &S3Config::accessKey, "");
}


/****************************************************************************/
/* S3 API :: RANGE                                                          */
/****************************************************************************/

S3Api::Range S3Api::Range::Full(0);

S3Api::Range::
Range(uint64_t aSize)
    : offset(0), size(aSize)
{}

S3Api::Range::
Range(uint64_t aOffset, uint64_t aSize)
    : offset(aOffset), size(aSize)
{}

uint64_t
S3Api::Range::
endPos()
    const
{
    return (offset + size - 1);
}

void
S3Api::Range::
adjust(size_t downloaded)
{
    if (downloaded > size) {
        throw ML::Exception("excessive adjustment size: downloaded %zu size %lu",
                            downloaded, size);
    }
    offset += downloaded;
    size -= downloaded;
}

string
S3Api::Range::
headerValue()
    const
{
    return (string("bytes=")
            + std::to_string(offset) + "-" + std::to_string(endPos()));
}

bool
S3Api::Range::
operator == (const Range & other)
    const
{
    return offset == other.offset && size == other.size;
}

bool
S3Api::Range::
operator != (const Range & other)
    const
{
    return !(*this == other);
}


/****************************************************************************/
/* S3 API :: REQUEST                                                        */
/****************************************************************************/

S3Api::Request::
Request()
    : downloadRange(0)
{
}

bool
S3Api::Request::
useRange()
    const
{
    /* The "Range" header is only useful with GET and when the range
       is explicitly specified. The use of Range::Full means that we
       always request the full body, even during retries. This is
       mainly useful for requests on non-object urls, where that
       header is ignored by the S3 servers. */
    return (verb == "GET" && downloadRange != Range::Full);
}

string
S3Api::Request::
makeUrl()
    const
{
    if (resource.find("//") != string::npos) {
        throw ML::Exception("resource has double slash: " + resource);
    }
    size_t spacePos = resource.find(" ");
    if (spacePos != string::npos) {
        throw ML::Exception("resource '" + resource + "' contains an"
                            " unescaped space at position "
                            + to_string(spacePos));
    }

    string url = resource;
    bool hasParams(false);
    if (subResource.size() > 0) {
        hasParams = true;
        url += "?" + subResource;
    }
    for (const auto & param: queryParams) {
        if (hasParams) {
            url += "&";
        }
        else {
            url += "?";
            hasParams = true;
        }
        url += (AwsApi::uriEncode(param.first)
                + "=" + AwsApi::uriEncode(param.second));
    }

    return url;
}

/****************************************************************************/
/* S3 API :: RESPONSE                                                       */
/****************************************************************************/

S3Api::Response::
Response()
    : code_(0)
{
}

const string &
S3Api::Response::
body()
    const
{
    if (code_ < 200 || code_ >= 300)
        throw ML::Exception("invalid http code returned");
    return body_;
}

const string &
S3Api::Response::
headers()
    const
{
    return headers_;
}

map<string, string>
S3Api::Response::
parsedHeaders()
    const
{
    HttpHeader header;
    header.parse(headers_, false);
    return std::move(header.headers);
}

/****************************************************************************/
/* S3 API :: OBJECT METADATA                                                */
/****************************************************************************/

S3Api::ObjectMetadata::
ObjectMetadata()
    : redundancy(REDUNDANCY_DEFAULT),
      serverSideEncryption(SSE_NONE),
      numRequests(8)
{
}

S3Api::ObjectMetadata::
ObjectMetadata(Redundancy redundancy)
    : redundancy(redundancy),
      serverSideEncryption(SSE_NONE),
      numRequests(8)
{
}

RestParams
S3Api::ObjectMetadata::
getRequestHeaders()
    const
{
    RestParams result;
    Redundancy redundancy = this->redundancy;

    if (redundancy == REDUNDANCY_DEFAULT)
        redundancy = defaultRedundancy;

    if (redundancy == REDUNDANCY_REDUCED)
        result.push_back({"x-amz-storage-class", "REDUCED_REDUNDANCY"});
    else if(redundancy == REDUNDANCY_GLACIER)
        result.push_back({"x-amz-storage-class", "GLACIER"});
    if (serverSideEncryption == SSE_AES256)
        result.push_back({"x-amz-server-side-encryption", "AES256"});
    if (contentType != "")
        result.push_back({"Content-Type", contentType});
    if (contentEncoding != "")
        result.push_back({"Content-Encoding", contentEncoding});
    if (acl != "")
        result.push_back({"x-amz-acl", acl});
    for (auto md: metadata) {
        result.push_back({"x-amz-meta-" + md.first, md.second});
    }
    return result;
}


/****************************************************************************/
/* S3 API :: MULTI PART UPLOAD PART                                         */
/****************************************************************************/

S3Api::MultiPartUploadPart::
MultiPartUploadPart()
    : partNumber(0), done(false)
{
}

void
S3Api::MultiPartUploadPart::
fromXml(tinyxml2::XMLElement * element)
{
    partNumber = extract<int>(element, "PartNumber");
    lastModified = extract<string>(element, "LastModified");
    etag = extract<string>(element, "ETag");
    size = extract<uint64_t>(element, "Size");
    done = true;
}


/****************************************************************************/
/* S3 API :: OBJECT INFO                                                    */
/****************************************************************************/

S3Api::ObjectInfo::
ObjectInfo(tinyxml2::XMLNode * element)
{
    size = extract<uint64_t>(element, "Size");
    key  = extract<string>(element, "Key");
    string lastModifiedStr = extract<string>(element, "LastModified");
    lastModified = Date::parseIso8601DateTime(lastModifiedStr);
    etag = extract<string>(element, "ETag");

    if (pathExists(element, "Owner/ID")) {
        ownerId = extract<string>(element, "Owner/ID");
    }

    ownerName = extractDef<string>(element, "Owner/DisplayName", "");
    storageClass = extract<string>(element, "StorageClass");
    exists = true;
}

S3Api::ObjectInfo::
ObjectInfo(const S3Api::Response & response)
{
    exists = true;
    auto headers = response.parsedHeaders();
    lastModified = Date::parse(headers.at("last-modified"),
                               "%a, %e %b %Y %H:%M:%S %Z");
    size = stoll(headers.at("content-length"));
    etag = headers.at("etag");
    storageClass = ""; // Not available in headers
    ownerId = "";      // Not available in headers
    ownerName = "";    // Not available in headers
}


/****************************************************************************/
/* S3 API                                                                   */
/****************************************************************************/

double
S3Api::
defaultBandwidthToServiceMbps = 20.0;

string
S3Api::
s3EscapeResource(const string & str)
{
    if (str.size() == 0) {
        throw ML::Exception("empty str name");
    }

    if (str[0] != '/') {
        throw ML::Exception("resource name must start with a '/'");
    }

    return quoteUrl(str);
}

S3Api::
S3Api()
{
    bandwidthToServiceMbps = defaultBandwidthToServiceMbps;
}

S3Api::
S3Api(const string & accessKeyId,
      const string & accessKey,
      double bandwidthToServiceMbps,
      const string & defaultProtocol,
      const string & serviceUri)
    : accessKeyId(accessKeyId),
      accessKey(accessKey),
      defaultProtocol(defaultProtocol),
      serviceUri(serviceUri),
      bandwidthToServiceMbps(bandwidthToServiceMbps)
{
}

void
S3Api::
init(const string & accessKeyId,
     const string & accessKey,
     double bandwidthToServiceMbps,
     const string & defaultProtocol,
     const string & serviceUri)
{
    this->accessKeyId = accessKeyId;
    this->accessKey = accessKey;
    this->defaultProtocol = defaultProtocol;
    this->serviceUri = serviceUri;
    this->bandwidthToServiceMbps = bandwidthToServiceMbps;
}

void
S3Api::
perform(S3Api::Request && rq, const OnResponse & onResponse)
    const
{
    string protocol = defaultProtocol;
    if (protocol.length() == 0) {
        throw ML::Exception("attempt to perform s3 request without a "
                            "default protocol. (Could be caused by S3Api"
                            " initialisation with the empty constructor.)");
    }

    auto state = make_shared<S3RequestState>(*this, std::move(rq), onResponse);
    performStateRequest(state);
}

S3Api::Response
S3Api::
performSync(S3Api::Request && rq)
    const
{
#if 1
    /* Temporary version relying on ML::futex_XXX since std::future and
       std::promise seems to be problematic in gcc 4.6 */
    S3Api::Response result;
    ML::ExceptionPtrHandler excPtr;
    std::atomic<int> done(0);

    auto onResponse = [&] (S3Api::Response && response,
                           std::exception_ptr newExc) {
        if (newExc) {
            excPtr.takeException(std::move(newExc));
        }
        else {
            result = std::move(response);
        }
        done = 1;
        ML::futex_wake(done);
    };
    perform(std::move(rq), onResponse);

    while (!done) {
        ML::futex_wait(done, done);
    }
    excPtr.rethrowIfSet();

    return result;
#else
    std::promise<S3Api::Response> respPromise;

    auto onResponse = [&] (S3Api::Response && response,
                           std::exception_ptr excPtr) {
        if (excPtr) {
            respPromise.set_exception(std::move(excPtr));
        }
        else {
            respPromise.set_value(std::move(response));
        }
    };
    perform(std::move(rq), onResponse);

    auto respFuture = respPromise.get_future();
    respFuture.wait();

    return respFuture.get();
#endif
}

S3Api::Response
S3Api::
head(const string & bucket,
     const string & resource,
     const string & subResource,
     const RestParams & headers,
     const RestParams & queryParams)
    const
{
    return headEscaped(bucket, s3EscapeResource(resource), subResource,
                       headers, queryParams);
}

S3Api::Response
S3Api::
headEscaped(const string & bucket,
            const string & resource,
            const string & subResource,
            const RestParams & headers,
            const RestParams & queryParams)
    const
{
    S3Api::Request request;
    request.verb = "HEAD";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;

    return performSync(std::move(request));
}

S3Api::Response
S3Api::
get(const string & bucket,
    const string & resource,
    const Range & downloadRange,
    const string & subResource,
    const RestParams & headers,
    const RestParams & queryParams)
    const
{
    return getEscaped(bucket, s3EscapeResource(resource), downloadRange,
                      subResource, headers, queryParams);
}

S3Api::Response
S3Api::
getEscaped(const string & bucket,
           const string & resource,
           const Range & downloadRange,
           const string & subResource,
           const RestParams & headers,
           const RestParams & queryParams)
    const
{
    S3Api::Request request;
    request.verb = "GET";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.downloadRange = downloadRange;

    return performSync(std::move(request));
}

void
S3Api::
getAsync(const OnResponse & onResponse,
         const string & bucket,
         const string & resource,
         const Range & downloadRange,
         const string & subResource,
         const RestParams & headers,
         const RestParams & queryParams)
    const
{
    getEscapedAsync(onResponse, bucket, s3EscapeResource(resource),
                    downloadRange, subResource, headers,
                    queryParams);
}

void
S3Api::
getEscapedAsync(const S3Api::OnResponse & onResponse,
                const string & bucket,
                const string & resource,
                const Range & downloadRange,
                const string & subResource,
                const RestParams & headers,
                const RestParams & queryParams)
    const
{
    S3Api::Request request;
    request.verb = "GET";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.downloadRange = downloadRange;

    perform(std::move(request), onResponse);
}

/** Perform a POST request from end to end. */
S3Api::Response
S3Api::
post(const string & bucket,
     const string & resource,
     const string & subResource,
     const RestParams & headers,
     const RestParams & queryParams,
     const HttpRequest::Content & content)
    const
{
    return postEscaped(bucket, s3EscapeResource(resource), subResource,
                       headers, queryParams, content);
}

S3Api::Response
S3Api::
postEscaped(const string & bucket,
            const string & resource,
            const string & subResource,
            const RestParams & headers,
            const RestParams & queryParams,
            const HttpRequest::Content & content)
    const
{
    S3Api::Request request;
    request.verb = "POST";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.content = content;

    return performSync(std::move(request));
}

S3Api::Response
S3Api::
putEscaped(const string & bucket,
           const string & resource,
           const string & subResource,
           const RestParams & headers,
           const RestParams & queryParams,
           const HttpRequest::Content & content,
           bool withMD5)
    const
{
    S3Api::Request request;
    request.verb = "PUT";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.content = content;
    if (withMD5) {
        auto b64Digest
            = AwsApi::base64EncodeDigest(AwsApi::md5Digest(content.str));
        request.contentMD5 = std::move(b64Digest);
    }

    return performSync(std::move(request));
}

void
S3Api::
putAsync(const OnResponse & onResponse,
         const string & bucket,
         const string & resource,
         const string & subResource,
         const RestParams & headers,
         const RestParams & queryParams,
         const HttpRequest::Content & content,
         bool withMD5)
    const
{
    putEscapedAsync(onResponse, bucket, s3EscapeResource(resource),
                    subResource, headers, queryParams, content, withMD5);
}

void
S3Api::
putEscapedAsync(const OnResponse & onResponse,
                const string & bucket,
                const string & resource,
                const string & subResource,
                const RestParams & headers,
                const RestParams & queryParams,
                const HttpRequest::Content & content,
                bool withMD5)
    const
{
    S3Api::Request request;
    request.verb = "PUT";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;
    request.content = content;
    if (withMD5) {
        auto b64Digest
            = AwsApi::base64EncodeDigest(AwsApi::md5Digest(content.str));
        request.contentMD5 = std::move(b64Digest);
    }

    perform(std::move(request), onResponse);
}

S3Api::Response
S3Api::
erase(const string & bucket,
      const string & resource,
      const string & subResource,
      const RestParams & headers,
      const RestParams & queryParams)
    const
{
    return eraseEscaped(bucket, s3EscapeResource(resource), subResource,
                        headers, queryParams);
}

S3Api::Response
S3Api::
eraseEscaped(const string & bucket,
             const string & resource,
             const string & subResource,
             const RestParams & headers,
             const RestParams & queryParams)
    const
{
    S3Api::Request request;
    request.verb = "DELETE";
    request.bucket = bucket;
    request.resource = resource;
    request.subResource = subResource;
    request.headers = headers;
    request.queryParams = queryParams;

    return performSync(std::move(request));
}

pair<bool,string>
S3Api::isMultiPartUploadInProgress(
    const string & bucket,
    const string & resource)
    const
{
    // Contains the resource without the leading slash
    string outputPrefix(resource, 1);

    // Check if there is already a multipart upload in progress
    auto inProgressReq = get(bucket, "/", Range::Full, "uploads", {},
                             { { "prefix", outputPrefix } });
    if (inProgressReq.code_ != 200)
        throw ML::Exception("invalid http code returned");
    //cerr << xmlAsStr(makeBodyXml(inProgressReq)) << endl;

    auto inProgress = makeBodyXml(inProgressReq);

    using namespace tinyxml2;

    XMLHandle handle(*inProgress);

    auto upload
        = handle
        .FirstChildElement("ListMultipartUploadsResult")
        .FirstChildElement("Upload")
        .ToElement();

    string uploadId;
    vector<MultiPartUploadPart> parts;


    for (; upload; upload = upload->NextSiblingElement("Upload"))
    {
        XMLHandle uploadHandle(upload);

        auto key = extract<string>(upload, "Key");

        if (key != outputPrefix)
            continue;

        // Already an upload in progress
        string uploadId = extract<string>(upload, "UploadId");

        return make_pair(true,uploadId);
    }
    return make_pair(false,"");
}

S3Api::MultiPartUpload
S3Api::
obtainMultiPartUpload(const string & bucket,
                      const string & resource,
                      const ObjectMetadata & metadata,
                      UploadRequirements requirements)
    const
{
    string escapedResource = s3EscapeResource(resource);
    // Contains the resource without the leading slash
    string outputPrefix(resource, 1);

    string uploadId;
    vector<MultiPartUploadPart> parts;

    if (requirements != UR_FRESH) {

        // Check if there is already a multipart upload in progress
        auto inProgressReq = get(bucket, "/", Range::Full, "uploads", {},
                                 { { "prefix", outputPrefix } });
        if (inProgressReq.code_ != 200)
            throw ML::Exception("invalid http code returned");

        //cerr << "in progress requests:" << endl;
        //cerr << xmlAsStr(makeBodyXml(inProgressReq)) << endl;

        auto inProgress = makeBodyXml(inProgressReq);

        using namespace tinyxml2;

        XMLHandle handle(*inProgress);

        auto upload
            = handle
            .FirstChildElement("ListMultipartUploadsResult")
            .FirstChildElement("Upload")
            .ToElement();

        // uint64_t partSize = 0;
        uint64_t currentOffset = 0;

        for (; upload; upload = upload->NextSiblingElement("Upload")) {
            XMLHandle uploadHandle(upload);

            auto key = extract<string>(upload, "Key");

            if (key != outputPrefix)
                continue;

            // Already an upload in progress
            string uploadId = extract<string>(upload, "UploadId");

            // From here onwards is only useful if we want to continue a half-finished
            // upload.  Instead, we will delete it to avoid problems with creating
            // half-finished files when we don't know what we're doing.

            auto deletedInfo = eraseEscaped(bucket, escapedResource,
                                            "uploadId=" + uploadId);

            continue;

            // TODO: check metadata, etc
            auto inProgressRq = getEscaped(bucket, escapedResource, Range::Full,
                                           "uploadId=" + uploadId);
            if (inProgressRq.code_ != 200)
                throw ML::Exception("invalid http code returned");

            auto inProgressInfo = makeBodyXml(inProgressRq);
            XMLHandle handle(*inProgressInfo);

            auto foundPart
                = handle
                .FirstChildElement("ListPartsResult")
                .FirstChildElement("Part")
                .ToElement();

            int numPartsDone = 0;
            uint64_t biggestPartSize = 0;
            for (; foundPart;
                 foundPart = foundPart->NextSiblingElement("Part"),
                     ++numPartsDone) {
                MultiPartUploadPart currentPart;
                currentPart.fromXml(foundPart);
                if (currentPart.partNumber != numPartsDone + 1) {
                    //cerr << "missing part " << numPartsDone + 1 << endl;
                    // from here we continue alone
                    break;
                }
                currentPart.startOffset = currentOffset;
                currentOffset += currentPart.size;
                biggestPartSize = std::max(biggestPartSize, currentPart.size);
                parts.push_back(currentPart);
            }

            // partSize = biggestPartSize;

            //cerr << "numPartsDone = " << numPartsDone << endl;
            //cerr << "currentOffset = " << currentOffset
            //     << "dataSize = " << dataSize << endl;
        }
    }

    if (uploadId.empty()) {
        //cerr << "getting new ID" << endl;

        RestParams headers = metadata.getRequestHeaders();
        auto result = postEscaped(bucket, escapedResource,
                                  "uploads", headers);
        if (result.code_ != 200)
            throw ML::Exception("invalid http code returned");

        auto xmlResult = makeBodyXml(result);
        uploadId
            = extract<string>(xmlResult, "InitiateMultipartUploadResult/UploadId");

        //cerr << "new upload = " << uploadId << endl;
    }
        //return;

    MultiPartUpload result;
    result.parts.swap(parts);
    result.id = uploadId;
    return result;
}

string
S3Api::
finishMultiPartUpload(const string & bucket,
                      const string & resource,
                      const string & uploadId,
                      const vector<string> & etags)
    const
{
    using namespace tinyxml2;
    // Finally, send back a response to join the parts together
    ExcAssert(etags.size() > 0);

    XMLDocument joinRequest;
    auto r = joinRequest.InsertFirstChild(joinRequest.NewElement("CompleteMultipartUpload"));
    for (unsigned i = 0;  i < etags.size();  ++i) {
        auto n = r->InsertEndChild(joinRequest.NewElement("Part"));
        n->InsertEndChild(joinRequest.NewElement("PartNumber"))
            ->InsertEndChild(joinRequest.NewText(ML::format("%d", i + 1).c_str()));
        n->InsertEndChild(joinRequest.NewElement("ETag"))
            ->InsertEndChild(joinRequest.NewText(etags[i].c_str()));
    }

    string escapedResource = s3EscapeResource(resource);

    HttpRequest::Content xmlReq(xmlDocumentAsString(joinRequest));
    auto joinResponse
        = postEscaped(bucket, escapedResource, "uploadId=" + uploadId,
                      {}, {}, xmlReq);
    if (joinResponse.code_ != 200)
        throw ML::Exception("invalid http code returned");

    //cerr << xmlAsStr(makeBodyXml(joinResponse)) << endl;

    auto joinResponseXml = makeBodyXml(joinResponse);

    try {

        string etag = extract<string>(joinResponseXml,
                                      "CompleteMultipartUploadResult/ETag");
        return etag;
    } catch (const std::exception & exc) {
        cerr << ("--- request is\n" + xmlDocumentAsString(joinRequest) + "\n"
                 "--- response is\n" + joinResponse.body_ + "\n(end)\n"
                 + "error completing multipart upload: " + exc.what() + "\n");
        throw;
    }
}

void
S3Api::
forEachObject(const string & bucket,
              const string & prefix,
              const OnObject & onObject,
              const OnSubdir & onSubdir,
              const string & delimiter,
              int depth,
              const string & startAt)
    const
{
    using namespace tinyxml2;

    string marker = startAt;
    // bool firstIter = true;
    do {
        //cerr << "Starting at " << marker << endl;

        RestParams queryParams;
        if (prefix != "")
            queryParams.push_back({"prefix", prefix});
        if (delimiter != "")
            queryParams.push_back({"delimiter", delimiter});
        if (marker != "")
            queryParams.push_back({"marker", marker});

        auto listingResult = get(bucket, "/", Range::Full, "",
                                 {}, queryParams);
        if (listingResult.code_ != 200)
            throw ML::Exception("invalid http code returned");
        auto listingResultXml = makeBodyXml(listingResult);
        string foundPrefix
            = extractDef<string>(listingResultXml, "ListBucketResult/Prefix", "");
        string truncated
            = extract<string>(listingResultXml, "ListBucketResult/IsTruncated");
        bool isTruncated = truncated == "true";
        marker = "";

        auto foundObject
            = XMLHandle(*listingResultXml)
            .FirstChildElement("ListBucketResult")
            .FirstChildElement("Contents")
            .ToElement();

        bool stop = false;

        for (int i = 0; onObject && foundObject;
             foundObject = foundObject->NextSiblingElement("Contents"), ++i) {
            ObjectInfo info(foundObject);

            string key = info.key;
            ExcAssertNotEqual(key, marker);
            marker = key;

            ExcAssertEqual(info.key.find(foundPrefix), 0);
            // cerr << "info.key: " + info.key + "; foundPrefix: " +foundPrefix + "\n";
            string basename(info.key, foundPrefix.length());

            if (!onObject(foundPrefix, basename, info, depth)) {
                stop = true;
                break;
            }
        }

        if (stop) return;

        auto foundDir
            = XMLHandle(*listingResultXml)
            .FirstChildElement("ListBucketResult")
            .FirstChildElement("CommonPrefixes")
            .ToElement();

        for (; onSubdir && foundDir;
             foundDir = foundDir->NextSiblingElement("CommonPrefixes")) {
            string dirName = extract<string>(foundDir, "Prefix");

            // Strip off the delimiter
            if (dirName.rfind(delimiter) == dirName.size() - delimiter.size()) {
                dirName = string(dirName, 0, dirName.size() - delimiter.size());
                ExcAssertEqual(dirName.find(prefix), 0);
                dirName = string(dirName, prefix.size());
            }
            if (onSubdir(foundPrefix, dirName, depth)) {
                string newPrefix = foundPrefix + dirName + "/";
                //cerr << "newPrefix = " << newPrefix << endl;
                forEachObject(bucket, newPrefix, onObject, onSubdir, delimiter,
                              depth + 1);
            }
        }

        // firstIter = false;
        if (!isTruncated)
            break;
    } while (marker != "");

    //cerr << "done scanning" << endl;
}

void
S3Api::
forEachObject(const string & uriPrefix,
              const OnObjectUri & onObject,
              const OnSubdir & onSubdir,
              const string & delimiter,
              int depth,
              const string & startAt)
    const
{
    string bucket, objectPrefix;
    std::tie(bucket, objectPrefix) = parseUri(uriPrefix);

    auto onObject2 = [&] (const string & prefix,
                          const string & objectName,
                          const ObjectInfo & info,
                          int depth)
        {
            string uri = "s3://" + bucket + "/" + prefix;
            if (objectName.size() > 0) {
                uri += objectName;
            }
            return onObject(uri, info, depth);
        };

    forEachObject(bucket, objectPrefix, onObject2, onSubdir, delimiter, depth, startAt);
}

S3Api::ObjectInfo
S3Api::
getObjectInfo(const string & bucket, const string & object,
              S3ObjectInfoTypes infos)
    const
{
    return ((infos & int(S3ObjectInfoTypes::FULL_EXTRAS)) != 0
            ? getObjectInfoFull(bucket, object)
            : getObjectInfoShort(bucket, object));
}

S3Api::ObjectInfo
S3Api::
getObjectInfoFull(const string & bucket, const string & object)
    const
{
    RestParams queryParams;
    queryParams.push_back({"prefix", object});

    auto listingResult = getEscaped(bucket, "/", Range::Full, "", {}, queryParams);

    if (listingResult.code_ != 200) {
        cerr << xmlAsStr(makeBodyXml(listingResult)) << endl;
        throw ML::Exception("error getting object");
    }

    auto listingResultXml = makeBodyXml(listingResult);

    auto foundObject
        = tinyxml2::XMLHandle(*listingResultXml)
        .FirstChildElement("ListBucketResult")
        .FirstChildElement("Contents")
        .ToElement();

    if (!foundObject)
        throw ML::Exception("object " + object + " not found in bucket "
                            + bucket);

    ObjectInfo info(foundObject);

    if(info.key != object){
        throw ML::Exception("object " + object + " not found in bucket "
                            + bucket);
    }
    return info;
}

S3Api::ObjectInfo
S3Api::
getObjectInfoShort(const string & bucket, const string & object)
    const
{
    auto res = head(bucket, "/" + object);
    if (res.code_ == 404) {
        throw ML::Exception("object " + object + " not found in bucket "
                            + bucket);
    }
    if (res.code_ != 200) {
        throw ML::Exception("error getting object");
    }
    return ObjectInfo(res);
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfo(const string & bucket,
                 const string & object,
                 S3ObjectInfoTypes infos)
    const
{
    return ((infos & int(S3ObjectInfoTypes::FULL_EXTRAS)) != 0
            ? tryGetObjectInfoFull(bucket, object)
            : tryGetObjectInfoShort(bucket, object));
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfoFull(const string & bucket, const string & object)
    const
{
    RestParams queryParams;
    queryParams.push_back({"prefix", object});

    auto listingResult = get(bucket, "/", Range::Full, "", {}, queryParams);
    if (listingResult.code_ != 200) { 
        cerr << xmlAsStr(makeBodyXml(listingResult)) << endl;
        throw ML::Exception("error getting object request: %ld",
                            listingResult.code_);
    }
    auto listingResultXml = makeBodyXml(listingResult);

    auto foundObject
        = tinyxml2::XMLHandle(*listingResultXml)
        .FirstChildElement("ListBucketResult")
        .FirstChildElement("Contents")
        .ToElement();

    if (!foundObject)
        return ObjectInfo();

    ObjectInfo info(foundObject);

    if (info.key != object) {
        return ObjectInfo();
    }

    return info;
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfoShort(const string & bucket, const string & object)
    const
{
    auto res = head(bucket, "/" + object);
    if (res.code_ == 404) {
        return ObjectInfo();
    }
    if (res.code_ != 200) {
        throw ML::Exception("error getting object");
    }

    return ObjectInfo(res);
}

S3Api::ObjectInfo
S3Api::
getObjectInfo(const string & uri, S3ObjectInfoTypes infos)
    const
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return getObjectInfo(bucket, object, infos);
}

S3Api::ObjectInfo
S3Api::
tryGetObjectInfo(const string & uri, S3ObjectInfoTypes infos)
    const
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return tryGetObjectInfo(bucket, object, infos);
}

void
S3Api::
eraseObject(const string & bucket,
            const string & object)
{
    Response response = erase(bucket, object);

    if (response.code_ != 204) {
        cerr << xmlAsStr(makeBodyXml(response)) << endl;
        throw ML::Exception("error erasing object request: %ld",
                            response.code_);
    }
}

bool
S3Api::
tryEraseObject(const string & bucket,
               const string & object)
{
    Response response = erase(bucket, object);

    if (response.code_ != 200) {
        return false;
    }

    return true;
}

void
S3Api::
eraseObject(const string & uri)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    eraseObject(bucket, object);
}

bool
S3Api::
tryEraseObject(const string & uri)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return tryEraseObject(bucket, object);
}

string
S3Api::
getPublicUri(const string & uri,
             const string & protocol)
{
    string bucket, object;
    std::tie(bucket, object) = parseUri(uri);
    return getPublicUri(bucket, object, protocol);
}

string
S3Api::
getPublicUri(const string & bucket,
             const string & object,
             const string & protocol)
{
    return protocol + "://" + bucket + ".s3.amazonaws.com/" + object;
}

/****************************************************************************/
/* STREAMING DOWNLOAD SOURCE                                                */
/****************************************************************************/

struct StreamingDownloadSource {
    StreamingDownloadSource(const string & urlStr)
    {
        owner = getS3ApiForUri(urlStr);

        string bucket, resource;
        std::tie(bucket, resource) = S3Api::parseUri(urlStr);
        downloader.reset(new S3Downloader(owner.get(),
                                          bucket, "/" + resource));
    }

    typedef char char_type;
    struct category
        : //input_seekable,
        boost::iostreams::input,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag
    { };

    std::streamsize read(char_type * s, std::streamsize n)
    {
        return downloader->read(s, n);
    }

    bool is_open() const
    {
        return !!downloader;
    }

    void close()
    {
        downloader->close();
        downloader.reset();
    }

private:
    std::shared_ptr<S3Api> owner;
    std::shared_ptr<S3Downloader> downloader;
};

std::unique_ptr<std::streambuf>
makeStreamingDownload(const string & uri)
{
    std::unique_ptr<std::streambuf> result;
    result.reset(new boost::iostreams::stream_buffer<StreamingDownloadSource>
                 (StreamingDownloadSource(uri),
                  131072));
    return result;
}


/****************************************************************************/
/* STREAMING UPLOAD SOURCE                                                  */
/****************************************************************************/

struct StreamingUploadSource {

    StreamingUploadSource(const string & urlStr,
                          const ML::OnUriHandlerException & excCallback,
                          const S3Api::ObjectMetadata & metadata)
    {
        owner = getS3ApiForUri(urlStr);

        string bucket, resource;
        std::tie(bucket, resource) = S3Api::parseUri(urlStr);
        uploader.reset(new S3Uploader(owner.get(), bucket, "/" + resource,
                                      excCallback, metadata));
    }

    typedef char char_type;
    struct category
        : public boost::iostreams::output,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    {
    };

    std::streamsize write(const char_type* s, std::streamsize n)
    {
        return uploader->write(s, n);
    }

    bool is_open() const
    {
        return !!uploader;
    }

    void close()
    {
        uploader->close();
        uploader.reset();
    }

private:
    std::shared_ptr<S3Api> owner;
    std::shared_ptr<S3Uploader> uploader;
};

std::unique_ptr<std::streambuf>
makeStreamingUpload(const string & uri,
                    const ML::OnUriHandlerException & onException,
                    const S3Api::ObjectMetadata & metadata)
{
    std::unique_ptr<std::streambuf> result;
    result.reset(new boost::iostreams::stream_buffer<StreamingUploadSource>
                 (StreamingUploadSource(uri, onException, metadata),
                  131072));
    return result;
}

std::pair<string, string>
S3Api::
parseUri(const string & uri)
{
    if (uri.find("s3://") != 0)
        throw ML::Exception("wrong scheme (should start with s3://)");
    string pathPart(uri, 5);
    string::size_type pos = pathPart.find('/');
    if (pos == string::npos)
        throw ML::Exception("couldn't find bucket name");
    string bucket(pathPart, 0, pos);
    string object(pathPart, pos + 1);

    return make_pair(bucket, object);
}

bool
S3Api::
forEachBucket(const OnBucket & onBucket)
    const
{
    using namespace tinyxml2;

    //cerr << "forEachObject under " << prefix << endl;

    auto listingResult = get("", "/", Range::Full, "");
    if (listingResult.code_ != 200)
        throw ML::Exception("invalid http code returned");
    auto listingResultXml = makeBodyXml(listingResult);
    auto foundBucket
        = XMLHandle(*listingResultXml)
        .FirstChildElement("ListAllMyBucketsResult")
        .FirstChildElement("Buckets")
        .FirstChildElement("Bucket")
        .ToElement();

    for (; onBucket && foundBucket;
         foundBucket = foundBucket->NextSiblingElement("Bucket")) {

        string foundName
            = extract<string>(foundBucket, "Name");
        if (!onBucket(foundName))
            return false;
    }

    return true;
}

void S3Api::setDefaultBandwidthToServiceMbps(double mbps){
    S3Api::defaultBandwidthToServiceMbps = mbps;
}

S3Api::Redundancy S3Api::defaultRedundancy = S3Api::REDUNDANCY_STANDARD;

void
S3Api::
setDefaultRedundancy(Redundancy redundancy)
{
    if (redundancy == REDUNDANCY_DEFAULT)
        throw ML::Exception("Can't set default redundancy as default");
    defaultRedundancy = redundancy;
}

S3Api::Redundancy
S3Api::
getDefaultRedundancy()
{
    return defaultRedundancy;
}

/** S3 support for filter_ostream opens.  Register the bucket name here, and
    you can open it directly from s3.
*/

/** Register S3 with the filter streams API so that a filter_stream can be used to
    treat an S3 object as a simple stream.
*/
struct RegisterS3Handler {
    static std::pair<std::streambuf *, bool>
    getS3Handler(const string & scheme,
                 const string & resource,
                 std::ios_base::open_mode mode,
                 const std::map<string, string> & options,
                 const ML::OnUriHandlerException & onException)
    {
        string::size_type pos = resource.find('/');
        if (pos == string::npos)
            throw ML::Exception("unable to find s3 bucket name in resource "
                                + resource);
        string bucket(resource, 0, pos);

        if (mode == ios::in) {
            return make_pair(makeStreamingDownload("s3://" + resource)
                             .release(),
                             true);
        }
        else if (mode == ios::out) {

            S3Api::ObjectMetadata md;
            for (auto & opt: options) {
                string name = opt.first;
                string value = opt.second;
                if (name == "redundancy" || name == "aws-redundancy") {
                    if (value == "STANDARD")
                        md.redundancy = S3Api::REDUNDANCY_STANDARD;
                    else if (value == "REDUCED")
                        md.redundancy = S3Api::REDUNDANCY_REDUCED;
                    else throw ML::Exception("unknown redundancy value " + value
                                             + " writing S3 object " + resource);
                }
                else if (name == "contentType" || name == "aws-contentType") {
                    md.contentType = value;
                }
                else if (name == "contentEncoding" || name == "aws-contentEncoding") {
                    md.contentEncoding = value;
                }
                else if (name == "acl" || name == "aws-acl") {
                    md.acl = value;
                }
                else if (name == "mode" || name == "compression"
                         || name == "compressionLevel") {
                    // do nothing
                }
                else if (name.find("aws-") == 0) {
                    throw ML::Exception("unknown aws option " + name + "=" + value
                                        + " opening S3 object " + resource);
                }
                else if(name == "num-threads")
                {
                    cerr << ("warning: use of obsolete 'num-threads' option"
                             " key\n");
                    md.numRequests = std::stoi(value);
                }
                else if(name == "num-requests")
                {
                    md.numRequests = std::stoi(value);
                }
                else {
                    cerr << "warning: skipping unknown S3 option "
                         << name << "=" << value << endl;
                }
            }

            return make_pair(makeStreamingUpload("s3://" + resource,
                                                 onException, md)
                             .release(),
                             true);
        }
        else throw ML::Exception("no way to create s3 handler for non in/out");
    }

    void registerBuckets()
    {
    }

    RegisterS3Handler()
    {
        ML::registerUriHandler("s3", getS3Handler);
    }

} registerS3Handler;

bool defaultBucketsRegistered = false;
std::mutex registerBucketsMutex;

tuple<string, string, string, string, string> getCloudCredentials()
{
    string filename = "";
    char* home;
    home = getenv("HOME");
    if (home != NULL)
        filename = home + string("/.cloud_credentials");
    if (filename != "" && ML::fileExists(filename)) {
        std::ifstream stream(filename.c_str());
        while (stream) {
            string line;

            getline(stream, line);
            if (line.empty() || line[0] == '#')
                continue;
            if (line.find("s3") != 0)
                continue;

            vector<string> fields = ML::split(line, '\t');

            if (fields[0] != "s3")
                continue;

            if (fields.size() < 4) {
                cerr << "warning: skipping invalid line in ~/.cloud_credentials: "
                     << line << endl;
                continue;
            }

            fields.resize(7);

            string version = fields[1];
            if (version != "1") {
                cerr << "warning: ignoring unknown version "
                     << version <<  " in ~/.cloud_credentials: "
                     << line << endl;
                continue;
            }

            string keyId = fields[2];
            string key = fields[3];
            string bandwidth = fields[4];
            string protocol = fields[5];
            string serviceUri = fields[6];

            return make_tuple(keyId, key, bandwidth, protocol, serviceUri);
        }
    }
    return make_tuple("", "", "", "", "");
}

string getEnv(const char * varName)
{
    const char * val = getenv(varName);
    return val ? val : "";
}

tuple<string, string, vector<string> >
getS3CredentialsFromEnvVar()
{
    return make_tuple(getEnv("S3_KEY_ID"), getEnv("S3_KEY"),
                      ML::split(getEnv("S3_BUCKETS"), ','));
}

/** Parse the ~/.cloud_credentials file and add those buckets in.

    The format of that file is as follows:
    1.  One entry per line
    2.  Tab separated
    3.  Comments are '#' in the first position
    4.  First entry is the name of the URI scheme (here, s3)
    5.  Second entry is the "version" of the configuration (here, 1)
        for forward compatibility
    6.  The rest of the entries depend upon the scheme; for s3 they are
        tab-separated and include the following:
        - Access key ID
        - Access key
        - Bandwidth from this machine to the server (MBPS)
        - Protocol (http)
        - S3 machine host name (s3.amazonaws.com)

    If S3_KEY_ID and S3_KEY environment variables are specified,
    they will be used first.
*/
void registerDefaultBuckets()
{
    if (defaultBucketsRegistered)
        return;

    std::unique_lock<std::mutex> guard(registerBucketsMutex);
    defaultBucketsRegistered = true;

    auto cloudCredentials = getCloudCredentials();
    if (get<0>(cloudCredentials) != "") {
        string keyId      = get<0>(cloudCredentials);
        string key        = get<1>(cloudCredentials);
        string bandwidth  = get<2>(cloudCredentials);
        string protocol   = get<3>(cloudCredentials);
        string serviceUri = get<4>(cloudCredentials);

        if (protocol == "")
            protocol = "http";
        if (bandwidth == "")
            bandwidth = "20.0";
        if (serviceUri == "")
            serviceUri = "s3.amazonaws.com";

        registerS3Buckets(keyId, key, boost::lexical_cast<double>(bandwidth),
                          protocol, serviceUri);
        return;
    }
    string keyId;
    string key;
    vector<string> buckets;

    std::tie(keyId, key, buckets) = getS3CredentialsFromEnvVar();
    if (keyId != "" && key != "") {
        if (buckets.empty()) {
            registerS3Buckets(keyId, key);
        }
        else {
            for (string bucket: buckets)
                registerS3Bucket(bucket, keyId, key);
        }
    }
    else
        cerr << "WARNING: registerDefaultBuckets needs either a "
            ".cloud_credentials or S3_KEY_ID and S3_KEY environment "
            " variables" << endl;
}

void registerS3Buckets(const string & accessKeyId,
                       const string & accessKey,
                       double bandwidthToServiceMbps,
                       const string & protocol,
                       const string & serviceUri)
{
    std::unique_lock<std::mutex> guard(s3ApiLock);
    globalS3Api.reset(new S3Api(accessKeyId, accessKey,
                                bandwidthToServiceMbps,
                                protocol, serviceUri));
}

void registerS3Bucket(const string & bucketName,
                      const string & accessKeyId,
                      const string & accessKey,
                      double bandwidthToServiceMbps,
                      const string & protocol,
                      const string & serviceUri)
{
    std::unique_lock<std::mutex> guard(s3ApiLock);

    auto it = specificS3Apis.find(bucketName);
    if (it == specificS3Apis.end()) {
        auto newApi = make_shared<S3Api>(accessKeyId, accessKey,
                                         bandwidthToServiceMbps,
                                         protocol, serviceUri);
        specificS3Apis.insert({bucketName, newApi});
        if (accessKeyId.size() > 0 && accessKey.size() > 0) {
            registerAwsCredentials(accessKeyId, accessKey);
        }
    }
}

std::shared_ptr<S3Api> getS3ApiForBucket(const string & bucketName)
{
    std::unique_lock<std::mutex> guard(s3ApiLock);

    auto s3Api = globalS3Api;
    auto it = specificS3Apis.find(bucketName);
    if (it != specificS3Apis.end()) {
        s3Api = it->second;
    }

    if (!s3Api) {
        throw ML::Exception("no s3 credentials registered for bucket " + bucketName);
    }

    return s3Api;
}

std::shared_ptr<S3Api> getS3ApiForUri(const string & uri)
{
    Url url(uri);

    string bucketName = url.host();
    string accessKeyId = url.username();
    if (accessKeyId.empty()) {
        return getS3ApiForBucket(bucketName);
    }

    string accessKey = url.password();
    if (accessKey.empty()) {
        accessKey = getAwsAccessKey(accessKeyId);
    }

    return make_shared<S3Api>(accessKeyId, accessKey);
}

} // namespace Datacratic
