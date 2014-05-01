/* http_client.h                                                   -*- C++ -*-
   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   An asynchronous HTTP client.

   HttpClient is meant to provide a featureful, generic and asynchronous HTTP
   client class. It supports strictly asynchronous (non-blocking) operations,
   HTTP pipelining and concurrent requests while enabling streaming responses
   via a callback mechanism. It is meant to be subclassed whenever a
   synchronous interface or a one-shot response mechanism is required. In
   general, the code should be complete enough that existing and similar
   classes could be subclassed gradually (HttpRestProxy, s3 internals). As a
   generic class, it does not make assumptions on the transferred contents.
   Finally, it is based on the interface of HttpRestProxy.

   Caveat:
   - cannot be used with a multi-threaded loop yet
   - has not been tweaked for performance yet
   - since those require header interpretation, there is not support for
     cookies per se
*/


/* TODO:
   blockers:
   - parser:
     - needs better validation (header key size, ...)
     - handling of multi-line headers
   - SSL support
   - connection timeout (Curl style)
   - socket timeout
   - auto disconnect (keep-alive)

   nice to have:
   - tunnelling
   - chunked encoding
 */

#pragma once

#include <deque>
#include <string>
#include <vector>

#include "jml/utils/ring_buffer.h"

#include "soa/jsoncpp/value.h"
#include "soa/service/message_loop.h"
#include "soa/service/http_header.h"
#include "soa/service/typed_message_channel.h"
#include "soa/service/tcp_socket.h"


namespace Datacratic {

struct HttpClientCallbacks;

/* MIME CONTENT */

/** Structure used to hold content for a MIME transaction. */
struct MimeContent {
    MimeContent()
        : data_(nullptr), size_(0), ownsContent_(false)
    {
    }

    MimeContent(const std::string & str, const std::string & contentType,
                bool copy = false)
        : data_(str.c_str()), size_(str.size()), contentType_(contentType),
          ownsContent_(copy)
    {
        if (copy) {
            copy_ = str;
            data_ = copy_.c_str();
        }
    }

    MimeContent(const char * data, uint64_t size,
                const std::string & contentType,
                bool copy = false)
        : data_(data), size_(size), contentType_(contentType),
          ownsContent_(copy)
    {
        if (copy) {
            copy_ = std::string(data_, size_);
            data_ = copy_.c_str();
        }
    }

    MimeContent(const Json::Value & content,
                const std::string & contentType = "application/json")
        : copy_(content.toString()),
          data_(copy_.c_str()), size_(copy_.size()),
          contentType_(contentType), ownsContent_(true)
    {
    }

    MimeContent(std::string && str, const std::string & contentType)
        : copy_(move(str)), data_(copy_.c_str()), size_(copy_.size()),
          contentType_(contentType), ownsContent_(true)
    {
    }

    MimeContent(MimeContent && other)
        : copy_(move(other.copy_)), data_(other.data_), size_(other.size_),
          contentType_(move(other.contentType_)),
          ownsContent_(other.ownsContent_)
    {
        if (ownsContent_) {
            data_ = copy_.c_str();
        }
    }

    MimeContent(const MimeContent & other)
        : copy_(other.copy_), data_(other.data_), size_(other.size_),
          contentType_(other.contentType_), ownsContent_(other.ownsContent_)
    {
        if (ownsContent_) {
            data_ = copy_.c_str();
        }
    }

    const char * data()
        const
    {
        return data_;
    }

    const uint64_t size()
        const
    {
        return size_;
    }

    const std::string & contentType()
        const
    {
        return contentType_;
    }

    bool isVoid()
        const
    {
        return (data_ == nullptr);
    }

    void clear()
    {
        copy_.clear();
        data_ = nullptr;
        size_ = 0;
        ownsContent_ = false;
    }

    MimeContent & operator = (const MimeContent & other)
    {
        copy_ = other.copy_;
        if (ownsContent_) {
            data_ = copy_.c_str();
        }
        else {
            data_ = other.data_;
        }
        size_ = other.size_;
        contentType_ = other.contentType_;
        ownsContent_ = other.ownsContent_;

        return *this;
    }

    MimeContent & operator = (MimeContent && other)
    {
        copy_ = move(other.copy_);
        if (ownsContent_) {
            data_ = copy_.c_str();
        }
        else {
            data_ = other.data_;
        }
        size_ = other.size_;
        contentType_ = move(other.contentType_);
        ownsContent_ = other.ownsContent_;

        return *this;
    }

private:
    std::string copy_;
    const char *data_;
    uint64_t size_;
    std::string contentType_;
    bool ownsContent_;
};


/* HTTPREQUEST */

/* Representation of an HTTP request. */
struct HttpRequest {
    HttpRequest()
        : timeout_(-1)
    {
    }

    HttpRequest(const std::string & verb, const std::string & url,
                const std::shared_ptr<HttpClientCallbacks> & callbacks,
                const MimeContent & content, const RestParams & headers,
                int timeout = -1)
        noexcept
        : verb_(verb), url_(url),
          headers_(headers), content_(content),
          callbacks_(callbacks), timeout_(timeout)
    {
        makeRequestStr();
    }

    void clear()
    {
        verb_.clear();
        url_.clear();
        headers_ = RestParams();
        content_.clear();
        callbacks_ = nullptr;
        timeout_ = -1;
        requestStr_.clear();
    }

    const std::string & requestStr()
        const
    {
        return requestStr_;
    }

    const MimeContent & content()
        const
    {
        return content_;
    }

    HttpClientCallbacks & callbacks()
        const
    {
        if (!callbacks_) {
            throw ML::Exception("callbacks not set");
        }

        return *callbacks_;
    }

private:
    void makeRequestStr() noexcept;

    std::string verb_;
    std::string url_;
    RestParams headers_;
    MimeContent content_;
    std::shared_ptr<HttpClientCallbacks> callbacks_;
    int timeout_;
    std::string requestStr_;
};


/* HTTP RESPONSE PARSER */

struct HttpResponseParser {
    typedef std::function<void (const std::string &,
                                int)> OnResponseStart;
    typedef std::function<void (const char *, size_t)> OnData;
    typedef std::function<void ()> OnDone;

    HttpResponseParser()
        : state_(0), remainingBody_(0)
    {}

    void clear();

    void feed(const char * data);
    void feed(const char * data, size_t size);

    uint64_t remainingBody() const
    {
        return remainingBody_;
    }

    OnResponseStart onResponseStart;
    OnData onHeader;
    OnData onData;
    OnDone onDone;

private:
    int state_;
    std::string buffer_;
    uint64_t remainingBody_;
};


/* HTTP CONNECTION */

struct HttpConnection : ClientTcpSocket {
    typedef std::function<void (int)> OnDone;

    static const uint64_t sendSize = 65536;

    enum HttpState {
        IDLE,
        HEADERS,
        BODY
    };

    HttpConnection();

    HttpConnection(const HttpConnection & other) = delete;

    ~HttpConnection();

    void clear();
    void perform(HttpRequest && request);

    const HttpRequest & request() const
    {
        return request_;
    }

    OnDone onDone;

private:
    virtual void onConnectionResult(ConnectionResult result,
                                    const std::vector<std::string> & msgs);
    virtual void onWriteResult(int error,
                               const std::string & written, size_t writtenSize);
    virtual void onReceivedData(const char * data, size_t size);
    virtual void onException(const std::exception_ptr & excPtr);

    void onParserResponseStart(const std::string & httpVersion, int code);
    void onParserHeader(const char * data, size_t size);
    void onParserData(const char * data, size_t size);
    void onParserDone();

    void handleEndOfRq(int code);

    HttpState state_;
    HttpRequest request_;
    HttpResponseParser parser_;
    size_t uploadOffset_;
};


/* HTTPCLIENT */

struct HttpClient : public MessageLoop {
    /* "baseUrl": scheme, hostname and port (scheme://hostname[:port]) that
       will be used as base for all requests
       "numParallels": number of requests that can be handled simultaneously
       "queueSize": size of the backlog of pending requests, after which
       operations will be refused */
    HttpClient(const std::string & baseUrl,
               int numParallel = 4, size_t queueSize = 32);
    HttpClient(HttpClient && other) = delete;
    HttpClient(const HttpClient & other) = delete;

    ~HttpClient();

    void shutdown();

    /** SSL checks */
    bool noSSLChecks;

    /** Use with servers that support HTTP pipelining */
    void enablePipelining();

    /** Enable outputting of debug information */
    void debug(bool debugOn);

    /** Performs a POST request, with "resource" as the location of the
     *  resource on the server indicated in "baseUrl". Query parameters
     *  should preferably be passed via "queryParams".
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool get(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("GET", resource, callbacks,
                              MimeContent(),
                              queryParams, headers, timeout);
    }

    /** Performs a POST request, using similar parameters as get with the
     * addition of "content", which defines the contents body and type.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool post(const std::string & resource,
              const std::shared_ptr<HttpClientCallbacks> & callbacks,
              const MimeContent & content = MimeContent(),
              const RestParams & queryParams = RestParams(),
              const RestParams & headers = RestParams(),
              int timeout = -1)
    {
        return enqueueRequest("POST", resource, callbacks, content,
                              queryParams, headers, timeout);
    }

    /** Performs a PUT request in a similar fashion to "post" above.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool put(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const MimeContent & content = MimeContent(),
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("PUT", resource, callbacks, content,
                              queryParams, headers, timeout);
    }

    HttpClient & operator = (HttpClient && other) = delete;
    HttpClient & operator = (const HttpClient & other) = delete;

private:

    /* Local */
    bool enqueueRequest(const std::string & verb,
                        const std::string & resource,
                        const std::shared_ptr<HttpClientCallbacks> & callbacks,
                        const MimeContent & content,
                        const RestParams & queryParams,
                        const RestParams & headers,
                        int timeout = -1);

    void handleQueueEvent(HttpRequest && rq);

    void handleHttpConnectionDone(HttpConnection & connection, int result);

#if 0
    void handleEvents();
    void handleEvent(const ::epoll_event & event);
    void handleTimerEvent();
    void handleMultiEvent(const ::epoll_event & event);

    void checkMultiInfos();

    static int socketCallback(CURL *e, curl_socket_t s, int what,
                              void *clientP, void *sockp);
    int onCurlSocketEvent(CURL *e, curl_socket_t s, int what, void *sockp);

    static int timerCallback(CURLM *multi, long timeoutMs, void *clientP);
    int onCurlTimerEvent(long timeout_ms);

    void addFd(int fd, bool isMod, int flags) const;
    void removeFd(int fd) const;
#endif

    HttpConnection * getConnection();
    void releaseConnection(HttpConnection * connection);

    std::string baseUrl_;

    bool debug_;

    std::vector<HttpConnection> connectionStash_;
    std::vector<HttpConnection *> avlConnections_;
    size_t nextAvail_;

    TypedMessageSink<HttpRequest> queue_; /* queued requests */
    std::deque<HttpRequest> inThreadQueue_; /* requests moved to the worker
                                             * thread */

    HttpConnection::OnDone onHttpConnectionDone_;
};


/* HTTPCLIENTCALLBACKS */

struct HttpClientCallbacks {
    typedef std::function<void (const HttpRequest &,
                                const std::string &,
                                int)> OnResponseStart;
    typedef std::function<void (const HttpRequest &,
                                const char *, size_t)> OnData;
    typedef std::function<void (const HttpRequest &,
                                int)> OnDone;

    HttpClientCallbacks(OnResponseStart onResponseStart = nullptr,
                        OnData onHeader = nullptr,
                        OnData onData = nullptr,
                        OnDone onDone = nullptr)
        : onResponseStart_(onResponseStart),
          onHeader_(onHeader), onData_(onData),
          onDone_(onDone)
    {
    }

    virtual ~HttpClientCallbacks()
    {
    }

    // static const std::string & errorMessage(HttpClientError errorCode);

    /* initiates a response */
    virtual void onResponseStart(const HttpRequest & rq,
                                 const std::string & httpVersion,
                                 int code);

    /* callback for header lines, one invocation per line */
    virtual void onHeader(const HttpRequest & rq,
                          const char * data, size_t size);

    /* callback for body data, one invocation per chunk */
    virtual void onData(const HttpRequest & rq,
                        const char * data, size_t size);

    /* callback for operation completions, implying that no other call will
     * be performed for the same request */
    virtual void onDone(const HttpRequest & rq,
                        int errorCode);

private:
    OnResponseStart onResponseStart_;
    OnData onHeader_;
    OnData onData_;
    OnDone onDone_;
};


/* SIMPLE CALLBACKS */

/* This class enables to simplify the interface use by clients which do not
 * need support for progressive responses. */
struct HttpClientSimpleCallbacks : public HttpClientCallbacks
{
    typedef std::function<void (const HttpRequest &,  /* request */
                                int,      /* error code */
                                int,                  /* status code */
                                std::string &&,       /* headers */
                                std::string &&)>      /* body */
        OnResponse;
    HttpClientSimpleCallbacks(const OnResponse & onResponse = nullptr);

    /* HttpClientCallbacks overrides */
    virtual void onResponseStart(const HttpRequest & rq,
                                 const std::string & httpVersion, int code);
    virtual void onHeader(const HttpRequest & rq,
                          const char * data, size_t size);
    virtual void onData(const HttpRequest & rq,
                        const char * data, size_t size);
    virtual void onDone(const HttpRequest & rq, int errorCode);

    virtual void onResponse(const HttpRequest & rq,
                            int error,
                            int status,
                            std::string && headers,
                            std::string && body);

private:
    OnResponse onResponse_;

    int statusCode_;
    std::string headers_;
    std::string body_;
};

} // namespace Datacratic
