/* http_client_v3.cc
   Wolfgang Sourdeau, December 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

/* TODO:
   - SSL
*/

#include <errno.h>
#include <sys/timerfd.h>

#include <string>

#include "boost/asio/connect.hpp"
#include "boost/asio/error.hpp"
#include "boost/asio/write.hpp"

#include "jml/arch/exception.h"
#include "jml/utils/exc_assert.h"

#include "soa/types/url.h"
#include "googleurl/src/gurl.h"
#include "googleurl/src/url_util.h"

#include "asio_utils.h"
#include "http_client.h"
#include "http_header.h"
#include "http_parsers.h"

#include "asio_http_client.h"

using namespace std;
using namespace boost;
using namespace Datacratic;

namespace {

static auto cancelledCode = make_error_code(asio::error::operation_aborted);
static auto eofCode = make_error_code(asio::error::eof);
static auto unreachableCode = make_error_code(asio::error::host_unreachable);

static asio::ip::tcp::resolver::iterator endIterator;

HttpClientError
translateError(const system::error_code & code)
{
    HttpClientError error;

    if (code == system::errc::success) {
        error = HttpClientError::None;
    }
    else if (code == system::errc::timed_out) {
        error = HttpClientError::Timeout;
    }
    else if (code == system::errc::host_unreachable) {
        error = HttpClientError::HostNotFound;
    }
    else if (code == system::errc::connection_refused
             || code == system::errc::network_unreachable) {
        error = HttpClientError::CouldNotConnect;
    }
    else if (code == system::errc::connection_reset
             || code == asio::error::eof) {
        error = HttpClientError::Unknown;
    }
    else {
        ::fprintf(stderr, "returning 'unknown' for code (%d): %s\n",
                  code.value(), code.message().c_str());
        error = HttpClientError::Unknown;
    }

    return error;
}

bool getExpectResponseBody(const HttpRequest & request)
{
    return (request.verb_ != "HEAD");
}

string
makeRequestStr(const HttpRequest & request)
{
    string requestStr;
    requestStr.reserve(10000);

    Url url(request.url_);
    requestStr = request.verb_ + " " + url.path();
    string query = url.query();
    if (query.size() > 0) {
        requestStr += "?" + query;
    }
    requestStr += " HTTP/1.1\r\n";
    requestStr += "Host: "+ url.host();
    int port = url.port();
    if (port > 0) {
        requestStr += ":" + to_string(port);
    }
    requestStr += "\r\nAccept: */*\r\n";
    for (const auto & header: request.headers_) {
        requestStr += header.first + ":" + header.second + "\r\n";
    }
    const auto & content = request.content_;
    if (!content.str.empty()) {
        requestStr += ("Content-Length: "
                       + to_string(content.str.size()) + "\r\n");
        requestStr += "Content-Type: " + content.contentType + "\r\n";
    }
    requestStr += "\r\n";

    return requestStr;
}

} // file scope


/* HTTP CONNECTION */

AsioHttpConnection::
AsioHttpConnection(asio::io_service & ioService)
    : socket_(ioService), connected_(false),
      responseState_(IDLE), requestEnded_(false), parsingEnded_(false),
      recvBuffer_(nullptr), recvBufferSize_(262144),
      resolver_(ioService), timeoutTimer_(ioService)
{
    // cerr << "AsioHttpConnection(): " << this << "\n";

    /* Apart with pipelining, there is no real interest in using the Nagle
       algorithm with HTTP, since we will want to send everything in one shot
       as soon as possible. */
    // setUseNagle(false);

    parser_.onResponseStart = [&] (const string & httpVersion,
                                   int code) {
        this->onParserResponseStart(httpVersion, code);
    };
    parser_.onHeader = [&] (const char * data, size_t size) {
        this->onParserHeader(data, size);
    };
    parser_.onData = [&] (const char * data, size_t size) {
        this->onParserData(data, size);
    };
    parser_.onDone = [&] (bool doClose) {
        this->onParserDone(doClose);
    };

    onReadSome_ = [&] (const system::error_code & ec, size_t bufferSize) {
        if (ec) {
            this->onReceiveError(ec, bufferSize);
        }
        else {
            this->onReceivedData(bufferSize);
        }
    };

    recvBuffer_ = new char[recvBufferSize_];
}

AsioHttpConnection::
~AsioHttpConnection()
{
    if (recvBuffer_) {
        delete[] recvBuffer_;
        recvBuffer_ = nullptr;
    }

    // cerr << "~AsioHttpConnection: " << this << "\n";
    cancelRequestTimer();
    if (responseState_ != IDLE) {
        ::fprintf(stderr,
                  "destroying non-idle connection: %d",
                  responseState_);
        abort();
    }
}

void
AsioHttpConnection::
clear()
{
    responseState_ = IDLE;
    requestEnded_ = false;
    request_.clear();
    rqData_.clear();
    lastCode_.clear();
}

void
AsioHttpConnection::
perform(HttpRequest && request)
{
    // cerr << "perform: " << this << endl;

    if (responseState_ != IDLE) {
        throw ML::Exception("%p: cannot process a request when state is not"
                            " idle: %d", this, responseState_);
    }

    request_ = move(request);

    if (connected_) {
        startSendingRequest();
    }
    else {
        resolveAndConnect();
    }
}

void
AsioHttpConnection::
resolveAndConnect()
{
    endpoints_.clear();
    currentEndpoint_ = 0;

    // cerr << " baseUrl : " + baseUrl_ + "\n";
    Url url(request_.url_);

    int port = url.url->EffectiveIntPort();
    if (url.hostIsIpAddress()) {
        asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(), port);
        endpoint.address(asio::ip::address_v4::from_string(url.host()));
        endpoints_.emplace_back(move(endpoint));
        connect();
    }
    else {
        asio::ip::tcp::resolver::query query(url.host(), to_string(port));
        auto onResolveFn = [&] (const system::error_code & ec,
                                asio::ip::tcp::resolver::iterator iterator) {
            if (ec) {
                handleEndOfRq(unreachableCode, true);
            }
            else {
                while (iterator != endIterator) {
                    endpoints_.push_back(*iterator);
                }
                connect();
            }
        };
        resolver_.async_resolve(query, onResolveFn);
    }
}

void
AsioHttpConnection::
connect()
{
    if (currentEndpoint_ < endpoints_.size()) {
        auto onConnectionResult = [&] (const system::error_code & ec) {
            if (ec) {
                if (ec == system::errc::connection_refused) {
                    currentEndpoint_++;
                    connect();
                }
                else {
                    handleEndOfRq(ec, false);
                }
            }
            else {
                connected_ = true;
                socket_.native_non_blocking(true);
                startSendingRequest();
            }
        };
        socket_.async_connect(endpoints_[currentEndpoint_],
                              onConnectionResult);
    }
    else {
        handleEndOfRq(unreachableCode, true);
    }
}

void
AsioHttpConnection::
startSendingRequest()
{
    /* This controls the maximum body size from which the body will be written
       separately from the request headers. This tend to improve performance
       by removing a potential reallocation and a large copy. 65536 appears to
       be a reasonable value but this would need to be tested on different
       setups. */
    static constexpr size_t TwoStepsThreshold(65536);

    parser_.setExpectBody(getExpectResponseBody(request_));
    rqData_ = makeRequestStr(request_);

    bool twoSteps(false);
    size_t totalSize(rqData_.size());

    const HttpRequest::Content & content = request_.content_;
    if (content.str.size() > 0) {
        if (content.str.size() < TwoStepsThreshold) {
            rqData_.append(content.str);
        }
        else {
            twoSteps = true;
        }
        totalSize += content.str.size();
    }

    // cerr << " twoSteps: " << twoSteps << endl;

    auto onWriteResult
        = [&] (const system::error_code & ec, std::size_t written) {
        if (ec) {
            this->onWriteError(ec, written);
        }
        else {
            this->onWrittenData(written);
        }
    };

    auto writeCompleteCond
        = [&, totalSize] (const system::error_code & ec,
                          std::size_t written) {
        // ::fprintf(stderr, "written: %d, total: %lu\n"
        //           written, totalSize);
        return written == totalSize;
    };

    responseState_ = PENDING;
    asio::const_buffers_1 writeBuffer(rqData_.c_str(), rqData_.size());
    if (twoSteps) {
        asio::const_buffers_1 writeBufferNext(content.str.c_str(),
                                              content.str.size());
        const_buffers_2 writeBuffers(writeBuffer, writeBufferNext);
        async_write(socket_, writeBuffers, writeCompleteCond, onWriteResult);
    }
    else {
        async_write(socket_, writeBuffer, writeCompleteCond, onWriteResult);
    }

    armRequestTimer();
}

void
AsioHttpConnection::
onWrittenData(size_t written)
{
    ExcAssertEqual(responseState_, PENDING);
    responseState_ = IDLE;
    parsingEnded_ = false;

    socket_.async_read_some(asio::buffer(recvBuffer_, recvBufferSize_),
                            onReadSome_);
}

void
AsioHttpConnection::
onReceivedData(size_t size)
{
    parser_.feed(recvBuffer_, size);
    if (!parsingEnded_) {
        socket_.async_read_some(asio::buffer(recvBuffer_, recvBufferSize_),
                                onReadSome_);
    }
}

void
AsioHttpConnection::
onWriteError(const system::error_code & ec, size_t bufferSize)
{
    if (ec != cancelledCode) {
        throw ML::Exception("unhandled error");
    }
}

void
AsioHttpConnection::
onReceiveError(const system::error_code & ec, size_t bufferSize)
{
    if (ec == eofCode) {
        this->handleEndOfRq(ec, true);
    }
    else if (ec != cancelledCode) {
        throw ML::Exception("unhandled error: " + ec.message());
    }
}

void
AsioHttpConnection::
onException(const exception_ptr & excPtr)
{
    cerr << "http client received exception\n";
    abort();
}

void
AsioHttpConnection::
onParserResponseStart(const string & httpVersion, int code)
{
    // ::fprintf(stderr, "%p: onParserResponseStart\n", this);
    request_.callbacks_->onResponseStart(request_, httpVersion, code);
}

void
AsioHttpConnection::
onParserHeader(const char * data, size_t size)
{
    // cerr << "onParserHeader: " << this << endl;
    request_.callbacks_->onHeader(request_, data, size);
}

void
AsioHttpConnection::
onParserData(const char * data, size_t size)
{
    // cerr << "onParserData: " << this << endl;
    request_.callbacks_->onData(request_, data, size);
}

void
AsioHttpConnection::
onParserDone(bool doClose)
{
    parsingEnded_ = true;
    handleEndOfRq(make_error_code(system::errc::success),
                  doClose);
}

/* This method handles end of requests: callback invocation, timer
 * cancellation etc. It may request the closing of the connection, in which
 * case the AsioHttpConnection will be ready for a new request only after
 * finalizeEndOfRq is invoked. */
void
AsioHttpConnection::
handleEndOfRq(const system::error_code & code, bool requireClose)
{
    if (requestEnded_) {
        // cerr << "ignoring extraneous end of request\n";
        ;
    }
    else {
        requestEnded_ = true;
        cancelRequestTimer();
        if (requireClose) {
            lastCode_ = code;
            requestClose();
        }
        else {
            finalizeEndOfRq(code);
        }
    }
}

void
AsioHttpConnection::
finalizeEndOfRq(const system::error_code & code)
{
    request_.callbacks_->onDone(request_, translateError(code));
    clear();
    onDone(code);
}

void
AsioHttpConnection::
requestClose()
{
    auto doCloseFn = [&] {
        doClose();
    };
    socket_.get_io_service().post(doCloseFn);
}

void
AsioHttpConnection::
doClose()
{
    socket_.close();
    connected_ = false;
    onClosed(false, {});
}

void
AsioHttpConnection::
onClosed(bool fromPeer, const std::vector<std::string> & msgs)
{
    if (fromPeer) {
        handleEndOfRq(make_error_code(system::errc::connection_reset),
                      false);
    }
    else {
        finalizeEndOfRq(lastCode_);
    }
}

void
AsioHttpConnection::
armRequestTimer()
{
    if (request_.timeout_ > 0) {
        auto secs = posix_time::seconds(request_.timeout_);
        timeoutTimer_.expires_from_now(secs);
        auto handleTimeoutEventFn = [&] (const system::error_code & ec) {
            this->handleTimeoutEvent(ec);
        };
        timeoutTimer_.async_wait(handleTimeoutEventFn);
    }
}

void
AsioHttpConnection::
cancelRequestTimer()
{
    timeoutTimer_.cancel();
}

void
AsioHttpConnection::
handleTimeoutEvent(const system::error_code & ec)
{
    if (!ec) {
        handleEndOfRq(make_error_code(asio::error::timed_out), true);
    }
}


/* HTTPCLIENT */

AsioHttpClient::
AsioHttpClient(asio::io_service & ioService,
               const string & baseUrl, int numParallel, size_t queueSize)
    : baseUrl_(baseUrl), nextAvail_(0)
{
    ExcAssert(baseUrl.compare(0, 8, "https://") != 0);

    queue_.reset(new HttpRequestQueue(ioService, queueSize));
    queue_->setOnNotify([&]() { this->handleQueueEvent(); });

    /* available connections */
    for (size_t i = 0; i < numParallel; i++) {
        auto connection = make_shared<AsioHttpConnection>(ioService);
        AsioHttpConnection * connectionPtr = connection.get();
        connection->onDone
            = [&, connectionPtr] (const system::error_code & result) {
            handleHttpConnectionDone(connectionPtr, result);
        };
        allConnections_.emplace_back(std::move(connection));
        avlConnections_.push_back(connectionPtr);
    }
}

AsioHttpClient::
~AsioHttpClient()
{
    // cerr << "~HttpClient: " << this << "\n";
}

void
AsioHttpClient::
enableDebug(bool value)
{
    debug_ = value;
}

void
AsioHttpClient::
enableSSLChecks(bool value)
{
}

void
AsioHttpClient::
enableTcpNoDelay(bool value)
{
}

void
AsioHttpClient::
enablePipelining(bool value)
{
    if (value) {
        throw ML::Exception("pipeline is not supported");
    }
}

bool
AsioHttpClient::
enqueueRequest(const string & verb, const string & resource,
               const std::shared_ptr<HttpClientCallbacks> & callbacks,
               const HttpRequest::Content & content,
               const RestParams & queryParams, const RestParams & headers,
               int timeout)
{
    // cerr << " enqueueRequest\n";

    string url = baseUrl_ + resource + queryParams.uriEscaped();
    HttpRequest request(verb, url, callbacks, content, headers, timeout);

    return queue_->push_back(std::move(request));
}

void
AsioHttpClient::
handleQueueEvent()
{
    // cerr << " handleQueueEvent\n";

    size_t numConnections = avlConnections_.size() - nextAvail_;
    // cerr << " numConnections: "  + to_string(numConnections) + "\n";
    if (numConnections > 0) {
        /* "0" has a special meaning for pop_front and must be avoided here */
        auto requests = queue_->pop_front(numConnections);
        for (auto request: requests) {
            AsioHttpConnection * conn = getConnection();
            if (!conn) {
                cerr << ("nextAvail_: "  + to_string(nextAvail_)
                         + "; num conn: "  + to_string(numConnections)
                         + "; num reqs: "  + to_string(requests.size())
                         + "\n");
                throw ML::Exception("inconsistency in count of available"
                                    " connections");
            }
            conn->perform(move(request));
        }
    }
}

void
AsioHttpClient::
handleHttpConnectionDone(AsioHttpConnection * connection,
                         const system::error_code & rc)
{
    auto requests = queue_->pop_front(1);
    if (requests.size() > 0) {
        // cerr << "emptying queue...\n";
        connection->perform(move(requests[0]));
    }
    else {
        releaseConnection(connection);
    }
}

AsioHttpConnection *
AsioHttpClient::
getConnection()
{
    AsioHttpConnection * conn;

    if (nextAvail_ < avlConnections_.size()) {
        conn = avlConnections_[nextAvail_];
        nextAvail_++;
    }
    else {
        conn = nullptr;
    }

    // cerr << " returning conn: " << conn << "\n";

    return conn;
}

void
AsioHttpClient::
releaseConnection(AsioHttpConnection * oldConnection)
{
    if (nextAvail_ > 0) {
        nextAvail_--;
        avlConnections_[nextAvail_] = oldConnection;
    }
}
