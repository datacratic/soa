/* http_client_v3.cc
   Wolfgang Sourdeau, December 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

/* TODO:
   - fixed failing tests:
     - timeouts
     - "expect 100 Continue"
     - expected and unexepected closing of sockets
   - async lookups
   - SSL
*/

#include <errno.h>
#include <sys/timerfd.h>

#include <string>

#include "boost/asio/connect.hpp"

#include "jml/arch/exception.h"
#include "jml/utils/exc_assert.h"

#include "soa/types/url.h"
#include "googleurl/src/gurl.h"
#include "googleurl/src/url_util.h"

#include "asio_threaded_loop.h"
#include "asio_utils.h"
#include "http_header.h"
#include "http_parsers.h"

#include "http_client_v3.h"

using namespace std;
using namespace boost::asio;
using namespace Datacratic;


namespace {

HttpClientError
translateError(const boost::system::error_code & code)
{
    HttpClientError error;

    using namespace boost::system::errc;

    if (code == success) {
        error = HttpClientError::None;
    }
    else if (code == timed_out) {
        error = HttpClientError::Timeout;
    }
    else if (code == host_unreachable) {
        error = HttpClientError::HostNotFound;
    }
    else if (code == connection_refused) {
        error = HttpClientError::CouldNotConnect;
    }
    else if (code == connection_reset) {
        error = HttpClientError::Unknown;
    }
    else {
        ::fprintf(stderr, "returning 'unknown' for code: %s\n",
                  code.message().c_str());
        error = HttpClientError::Unknown;
    }

    return error;
}

io_service &
getHTTPClientLoop()
{
    static AsioThreadedLoop loop;
    loop.startSync();

    return loop.getIoService();
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

HttpConnectionV3::
HttpConnectionV3(io_service & ioService,
                 const ip::tcp::endpoint & endpoint)
    : socket_(ioService), connected_(false),
      endpoint_(endpoint), responseState_(IDLE),
      requestEnded_(false), parsingEnded_(false),
      recvBuffer_(nullptr), recvBufferSize_(262144),
      timeoutFd_(-1)
{
    // cerr << "HttpConnectionV3(): " << this << "\n";

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

    onReceivedDataFn_ = [&] (const boost::system::error_code & ec,
                             size_t bufferSize) {
        this->onReceivedData(recvBuffer_, bufferSize);
    };

    recvBuffer_ = new char[recvBufferSize_];
}

HttpConnectionV3::
~HttpConnectionV3()
{
    if (recvBuffer_) {
        delete[] recvBuffer_;
        recvBuffer_ = nullptr;
    }

    // cerr << "~HttpConnectionV3: " << this << "\n";
    cancelRequestTimer();
    if (responseState_ != IDLE) {
        ::fprintf(stderr,
                  "destroying non-idle connection: %d",
                  responseState_);
        abort();
    }
}

void
HttpConnectionV3::
clear()
{
    responseState_ = IDLE;
    requestEnded_ = false;
    request_.clear();
    rqData_.clear();
    lastCode_.clear();
}

void
HttpConnectionV3::
perform(HttpRequest && request)
{
    // cerr << "perform: " << this << endl;

    if (responseState_ != IDLE) {
        throw ML::Exception("%p: cannot process a request when state is not"
                            " idle: %d", this, responseState_);
    }

    request_ = move(request);

    if (queueEnabled()) {
        startSendingRequest();
    }
    else {
        auto onConnectionResult = [&] (const boost::system::error_code & ec) {
            if (!ec) {
                connected_ = true;
                socket_.native_non_blocking(true);
                startSendingRequest();
            }
            else {
                handleEndOfRq(ec, false);
            }
        };
        socket_.async_connect(endpoint_, onConnectionResult);
    }
}

void
HttpConnectionV3::
startSendingRequest()
{
    /* This controls the maximum body size from which the body will be written
       separately from the request headers. This tend to improve performance
       by removing a potential allocation and a large copy. 65536 appears to
       be a reasonable value on my installation, but this would need to be
       tested on different setups. */
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

    auto onWriteResult = [&] (const boost::system::error_code & ec,
                              std::size_t written) {
        // cerr << " onWriteResult\n";
        if (ec) {
            throw ML::Exception("unhandled error");
        }
        ExcAssertEqual(responseState_, PENDING);
        responseState_ = IDLE;
        parsingEnded_ = false;

        socket_.async_read_some(boost::asio::buffer(recvBuffer_,
                                                    recvBufferSize_),
                                onReceivedDataFn_);
    };

    auto writeCompleteCond
        = [&, totalSize] (const boost::system::error_code & ec,
                          std::size_t written) {
        // ::fprintf(stderr, "written: %d, total: %lu\n"
        //           written, totalSize);
        return written == totalSize;
    };

    responseState_ = PENDING;
    const_buffers_1 writeBuffer(rqData_.c_str(), rqData_.size());
    if (twoSteps) {
        const_buffers_1 writeBufferNext(content.str.c_str(),
                                        content.str.size());
        const_buffers_2 writeBuffers(writeBuffer, writeBufferNext);
        async_write(socket_, writeBuffers, writeCompleteCond, onWriteResult);
    }
    else {
        const_buffers_1 writeBuffer(rqData_.c_str(), rqData_.size());
        async_write(socket_, writeBuffer, writeCompleteCond, onWriteResult);
    }

    armRequestTimer();
}

void
HttpConnectionV3::
onReceivedData(const char * data, size_t size)
{
    parser_.feed(data, size);
    if (!parsingEnded_) {
        socket_.async_read_some(boost::asio::buffer(recvBuffer_,
                                                    recvBufferSize_),
                                onReceivedDataFn_);
    }
}

void
HttpConnectionV3::
onException(const exception_ptr & excPtr)
{
    cerr << "http client received exception\n";
    abort();
}

void
HttpConnectionV3::
onParserResponseStart(const string & httpVersion, int code)
{
    // ::fprintf(stderr, "%p: onParserResponseStart\n", this);
    request_.callbacks_->onResponseStart(request_, httpVersion, code);
}

void
HttpConnectionV3::
onParserHeader(const char * data, size_t size)
{
    // cerr << "onParserHeader: " << this << endl;
    request_.callbacks_->onHeader(request_, data, size);
}

void
HttpConnectionV3::
onParserData(const char * data, size_t size)
{
    // cerr << "onParserData: " << this << endl;
    request_.callbacks_->onData(request_, data, size);
}

void
HttpConnectionV3::
onParserDone(bool doClose)
{
    parsingEnded_ = true;
    handleEndOfRq(make_error_code(boost::system::errc::success),
                  doClose);
}

/* This method handles end of requests: callback invocation, timer
 * cancellation etc. It may request the closing of the connection, in which
 * case the HttpConnectionV3 will be ready for a new request only after
 * finalizeEndOfRq is invoked. */
void
HttpConnectionV3::
handleEndOfRq(const boost::system::error_code & code, bool requireClose)
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
HttpConnectionV3::
finalizeEndOfRq(const boost::system::error_code & code)
{
    request_.callbacks_->onDone(request_, translateError(code));
    clear();
    onDone(code);
}

void
HttpConnectionV3::
requestClose()
{
    auto doCloseFn = [&] {
        doClose();
    };
    socket_.get_io_service().post(doCloseFn);
}

void
HttpConnectionV3::
doClose()
{
    socket_.close();
    connected_ = false;
    onClosed(false, {});
}

void
HttpConnectionV3::
onClosed(bool fromPeer, const std::vector<std::string> & msgs)
{
    if (fromPeer) {
        handleEndOfRq(make_error_code(boost::system::errc::connection_reset),
                      false);
    }
    else {
        finalizeEndOfRq(lastCode_);
    }
}

void
HttpConnectionV3::
armRequestTimer()
{
#if 0
    if (request_.timeout_ > 0) {
        if (timeoutFd_ == -1) {
            timeoutFd_ = timerfd_create(CLOCK_MONOTONIC,
                                        TFD_NONBLOCK | TFD_CLOEXEC);
            if (timeoutFd_ == -1) {
                throw ML::Exception(errno, "timerfd_create");
            }
            auto handleTimeoutEventCb = [&] (const struct epoll_event & event) {
                this->handleTimeoutEvent(event);
            };
            registerFdCallback(timeoutFd_, handleTimeoutEventCb);
            // cerr << " timeoutFd_: "  + to_string(timeoutFd_) + "\n";
            addFdOneShot(timeoutFd_, true, false);
            // cerr << "timer armed\n";
        }
        else {
            // cerr << "timer rearmed\n";
            modifyFdOneShot(timeoutFd_, true, false);
        }

        itimerspec spec;
        ::memset(&spec, 0, sizeof(itimerspec));

        spec.it_interval.tv_sec = 0;
        spec.it_value.tv_sec = request_.timeout_;
        int res = timerfd_settime(timeoutFd_, 0, &spec, nullptr);
        if (res == -1) {
            throw ML::Exception(errno, "timerfd_settime");
        }
    }
#endif
}

void
HttpConnectionV3::
cancelRequestTimer()
{
#if 0
    // cerr << "cancel request timer " << this << "\n";
    if (timeoutFd_ != -1) {
        // cerr << "  was active\n";
        removeFd(timeoutFd_);
        unregisterFdCallback(timeoutFd_, true);
        ::close(timeoutFd_);
        timeoutFd_ = -1;
    }
    // else {
    //     cerr << "  was not active\n";
    // }
#endif
}

void
HttpConnectionV3::
handleTimeoutEvent(const ::epoll_event & event)
{
    if (timeoutFd_ == -1) {
        return;
    }

    if ((event.events & EPOLLIN) != 0) {
        while (true) {
            uint64_t expiries;
            int res = ::read(timeoutFd_, &expiries, sizeof(expiries));
            if (res == -1) {
                if (errno == EAGAIN) {
                    break;
                }

                throw ML::Exception(errno, "read");
            }
        }
        handleEndOfRq(make_error_code(boost::system::errc::timed_out),
                      true);
    }
}


/* HTTPCLIENT */

HttpClientV3::
HttpClientV3(const string & baseUrl, int numParallel, size_t queueSize)
    : HttpClientImpl(baseUrl, numParallel, queueSize),
      baseUrl_(baseUrl), nextAvail_(0)
{
    ExcAssert(baseUrl.compare(0, 8, "https://") != 0);

    // cerr << " baseUrl : " + baseUrl_ + "\n";
    Url url(baseUrl_);

#if 0
    ip::tcp::resolver resolver(ioService);

    ip::tcp::resolver::query query(url.host(),
                                                to_string(url.url->EffectiveIntPort()));
    boost::system::error_code error;
    ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query, error);
    if (error) {
        throw ML::Exception("resolve error");
    }
#endif
    ip::address address(ip::address_v4::from_string("127.0.0.1"));
    ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(),
                               url.url->EffectiveIntPort());
    endpoint.address(address);

    io_service & ioService = getHTTPClientLoop();

    queue_.reset(new HttpRequestQueue(ioService, queueSize));
    queue_->setOnNotify([&]() { this->handleQueueEvent(); });

    /* available connections */
    for (size_t i = 0; i < numParallel; i++) {
        auto connection = make_shared<HttpConnectionV3>(ioService, endpoint);
        HttpConnectionV3 * connectionPtr = connection.get();
        connection->onDone
            = [&, connectionPtr] (const boost::system::error_code & result) {
            handleHttpConnectionDone(connectionPtr, result);
        };
        allConnections_.emplace_back(std::move(connection));
        avlConnections_.push_back(connectionPtr);
    }
}

HttpClientV3::
~HttpClientV3()
{
    // cerr << "~HttpClient: " << this << "\n";
}

void
HttpClientV3::
enableDebug(bool value)
{
    debug_ = value;
}

void
HttpClientV3::
enableSSLChecks(bool value)
{
}

void
HttpClientV3::
enableTcpNoDelay(bool value)
{
}

void
HttpClientV3::
sendExpect100Continue(bool value)
{
}

void
HttpClientV3::
enablePipelining(bool value)
{
    if (value) {
        throw ML::Exception("pipeline is not supported");
    }
}

bool
HttpClientV3::
enqueueRequest(const string & verb, const string & resource,
               const shared_ptr<HttpClientCallbacks> & callbacks,
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
HttpClientV3::
handleQueueEvent()
{
    // cerr << " handleQueueEvent\n";

    size_t numConnections = avlConnections_.size() - nextAvail_;
    // cerr << " numConnections: "  + to_string(numConnections) + "\n";
    if (numConnections > 0) {
        /* "0" has a special meaning for pop_front and must be avoided here */
        auto requests = queue_->pop_front(numConnections);
        for (auto request: requests) {
            HttpConnectionV3 * conn = getConnection();
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
HttpClientV3::
handleHttpConnectionDone(HttpConnectionV3 * connection,
                         const boost::system::error_code & rc)
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

HttpConnectionV3 *
HttpClientV3::
getConnection()
{
    HttpConnectionV3 * conn;

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
HttpClientV3::
releaseConnection(HttpConnectionV3 * oldConnection)
{
    if (nextAvail_ > 0) {
        nextAvail_--;
        avlConnections_[nextAvail_] = oldConnection;
    }
}
