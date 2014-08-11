#include <errno.h>
#include <sys/timerfd.h>

#include "jml/arch/exception.h"

#include "soa/types/url.h"
#include "soa/service/message_loop.h"
#include "soa/service/http_header.h"
#include "soa/service/http_parsers.h"

#include "http_client.h"


using namespace std;
using namespace Datacratic;


/* HTTP REQUEST */

void
HttpRequest::
makeRequestStr()
    noexcept
{
    Url url(url_);
    requestStr_.reserve(10000);
    requestStr_ = verb_ + " " + url.path();
    string query = url.query();
    if (query.size() > 0) {
        requestStr_ += "?" + query;
    }
    requestStr_ += " HTTP/1.1\r\n";
    requestStr_ += "Host: "+ url.host();
    int port = url.port();
    if (port > 0) {
        requestStr_ += ":" + to_string(port);
    }
    requestStr_ += "\r\nAccept: */*\r\n";
    for (const auto & header: headers_) {
        requestStr_ += header.first + ":" + header.second + "\r\n";
    }
    if (!content_.isVoid()) {
        requestStr_ += ("Content-Length: "
                        + to_string(content_.size()) + "\r\n");
        requestStr_ += "Content-Type: " + content_.contentType() + "\r\n";
    }
    requestStr_ += "\r\n";
}


#if 0
/* HTTPCLIENTERROR */

std::ostream &
Datacratic::
operator << (std::ostream & stream, HttpClientError error)
{
    return stream << HttpClientCallbacks::errorMessage(error);
}


/* HTTPCLIENTCALLBACKS */

const string &
HttpClientCallbacks::
errorMessage(HttpClientError errorCode)
{
    static const string none = "No error";
    static const string unknown = "Unknown error";
    static const string hostNotFound = "Host not found";
    static const string couldNotConnect = "Could not connect";
    static const string timeout = "Request timed out";

    switch (errorCode) {
    case HttpClientError::None:
        return none;
    case HttpClientError::Unknown:
        return unknown;
    case HttpClientError::Timeout:
        return timeout;
    case HttpClientError::HostNotFound:
        return hostNotFound;
    case HttpClientError::CouldNotConnect:
        return couldNotConnect;
    default:
        throw ML::Exception("invalid error code");
    };
}
#endif

void
HttpClientCallbacks::
startResponse(const HttpRequest & rq,
              const std::string & httpVersion,
              int code)
{
    // ::fprintf(stderr, "%p: startResponse\n", this);
    onResponseStart(rq, httpVersion, code);
}

void
HttpClientCallbacks::
feedHeader(const HttpRequest & rq,
               const char * data, size_t size)
{
    onHeader(rq, data, size);
}

void
HttpClientCallbacks::
feedBodyData(const HttpRequest & rq,
             const char * data, size_t size)
{
    onData(rq, data, size);
}

void
HttpClientCallbacks::
endResponse(const HttpRequest & rq, int errorCode)
{
    // ::fprintf(stderr, "%p: endResponse\n", this);
    onDone(rq, errorCode);
}

void
HttpClientCallbacks::
onResponseStart(const HttpRequest & rq,
                const string & httpVersion, int code)
{
    if (onResponseStart_)
        onResponseStart_(rq, httpVersion, code);
}

void
HttpClientCallbacks::
onHeader(const HttpRequest & rq, const char * data, size_t size)
{
    if (onHeader_)
        onHeader_(rq, data, size);
}

void
HttpClientCallbacks::
onData(const HttpRequest & rq, const char * data, size_t size)
{
    if (onData_)
        onData_(rq, data, size);
}

void
HttpClientCallbacks::
onDone(const HttpRequest & rq, int errorCode)
{
    if (onDone_)
        onDone_(rq, errorCode);
}


/* HTTP CONNECTION */

HttpConnection::
HttpConnection()
    : responseState_(IDLE), requestEnded_(false), lastCode_(0), timeoutFd_(-1)
{
    // cerr << "HttpConnection(): " << this << "\n";

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
}

HttpConnection::
~HttpConnection()
{
    // cerr << "~HttpConnection: " << this << "\n";
    cancelRequestTimer();
    if (responseState_ != IDLE) {
        ::fprintf(stderr,
                  "destroying non-idle connection: %d",
                  responseState_);
        abort();
    }
}

void
HttpConnection::
clear()
{
    responseState_ = IDLE;
    requestEnded_ = false;
    request_.clear();
    uploadOffset_ = 0;
    lastCode_ = 0;
}

void
HttpConnection::
perform(HttpRequest && request)
{
    // cerr << "perform: " << this << endl;

    if (responseState_ != IDLE) {
        throw ML::Exception("%p: cannot process a request when state is not"
                            " idle: %d", this, responseState_);
    }

    request_ = move(request);

    responseState_ = HEADERS;
    if (queueEnabled()) {
        write(request_.requestStr());
        armRequestTimer();
    }
    else {
        connect();
    }
}

void
HttpConnection::
onConnectionResult(ConnectionResult result, const vector<string> & msgs)
{
    // cerr << " onConnectionResult: " + to_string(result) + "\n";
    if (result == ConnectionResult::SUCCESS) {
        write(request_.requestStr());
        armRequestTimer();
    }
    else {
        cerr << " failure with result: "  + to_string(result) + "\n";
        handleEndOfRq(result, true);
    }
}

void
HttpConnection::
onWriteResult(int error, const string & written, size_t writtenSize)
{
    if (error == 0) {
        const MimeContent content = request_.content();
        if (responseState_ == HEADERS) {
            if (content.size() > 0) {
                responseState_ = BODY;
                uploadOffset_ = 0;
            }
            else {
                responseState_ = IDLE;
            }
        }
        else if (responseState_ == BODY) {
            uploadOffset_ += writtenSize;
        }
        else if (responseState_ != BODY) {
            throw ML::Exception("invalid state");
        }
        if (responseState_ == BODY) {
            uint64_t remaining = content.size() - uploadOffset_;
            uint64_t chunkSize = min(remaining, HttpConnection::sendSize);
            if (chunkSize == 0) {
                responseState_ = IDLE;
            }
            else {
                write(content.data() + uploadOffset_, chunkSize);
            }
        }
    }
    else {
        throw ML::Exception("unhandled error");
    }
}

void
HttpConnection::
onReceivedData(const char * data, size_t size)
{
    // cerr << "onReceivedData: " + string(data, size) + "\n";
    parser_.feed(data, size);
}

void
HttpConnection::
onException(const exception_ptr & excPtr)
{
    cerr << "http client received exception\n";
    abort();
}

void
HttpConnection::
onParserResponseStart(const string & httpVersion, int code)
{
    // ::fprintf(stderr, "%p: onParserResponseStart\n", this);
    request_.callbacks().startResponse(request_, httpVersion, code);
}

void
HttpConnection::
onParserHeader(const char * data, size_t size)
{
    // cerr << "onParserHeader: " << this << endl;
    request_.callbacks().feedHeader(request_, data, size);
}

void
HttpConnection::
onParserData(const char * data, size_t size)
{
    // cerr << "onParserData: " << this << endl;
    request_.callbacks().feedBodyData(request_, data, size);
}

void
HttpConnection::
onParserDone(bool doClose)
{
    handleEndOfRq(0, doClose);
}

/* This method handles end of requests: callback invocation, timer
 * cancellation etc. It may request the closing of the connection, in which
 * case the HttpConnection will be ready for a new request only after
 * finalizeEndOfRq is invoked. */
void
HttpConnection::
handleEndOfRq(int code, bool requireClose)
{
    // ::fprintf(stderr, "%p: handleEndOfRq: %d, %d, %d\n",
    //           this, code, requireClose, requestEnded_);
    if (requestEnded_) {
        // cerr << "ignoring extraneous end of request\n";
        ;
    }
    else {
        requestEnded_ = true;

        // cerr << "handleEndOfRq: " << this << endl;
        cancelRequestTimer();

        if (requireClose) {
            lastCode_ = code;
            // cerr << "request close\n";
            requestClose();
        }
        else {
            finalizeEndOfRq(code);
        }
    }
}

void
HttpConnection::
finalizeEndOfRq(int code)
{
    request_.callbacks().endResponse(request_, code);
    clear();
    onDone(code);
}

void
HttpConnection::
onDisconnected(bool fromPeer, const std::vector<std::string> & msgs)
{
    if (fromPeer) {
        ;
    }
    else {
        // cerr << "disconnected...\n";
        finalizeEndOfRq(lastCode_);
    }
}

void
HttpConnection::
armRequestTimer()
{
    if (request_.timeout() > 0) {
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
            cerr << " timeoutFd_: "  + to_string(timeoutFd_) + "\n";
            addFdOneShot(timeoutFd_, true, false);
            cerr << "timer armed\n";
        }
        else {
            cerr << "timer rearmed\n";
            modifyFdOneShot(timeoutFd_, true, false);
        }

        itimerspec spec;
        ::memset(&spec, 0, sizeof(itimerspec));

        spec.it_interval.tv_sec = 0;
        spec.it_value.tv_sec = request_.timeout();
        int res = timerfd_settime(timeoutFd_, 0, &spec, nullptr);
        if (res == -1) {
            throw ML::Exception(errno, "timerfd_settime");
        }
    }
}

void
HttpConnection::
cancelRequestTimer()
{
    // cerr << "cancel request timer " << this << "\n";
    if (timeoutFd_ != -1) {
        // cerr << "  was active\n";
        removeFd(timeoutFd_);
        unregisterFdCallback(timeoutFd_);
        ::close(timeoutFd_);
        timeoutFd_ = -1;
    }
    // else {
    //     cerr << "  was not active\n";
    // }
}

void
HttpConnection::
handleTimeoutEvent(const ::epoll_event & event)
{
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
        // cerr << "ending request due to timeout\n";
        handleEndOfRq(ConnectionResult::TIMEOUT, true);
    }
}


/* HTTPCLIENT */

HttpClient::
HttpClient(const string & baseUrl, int numParallel, size_t queueSize)
    : MessageLoop(1, 0, -1),
      noSSLChecks(false),
      baseUrl_(baseUrl),
      debug_(false),
      avlConnections_(numParallel),
      nextAvail_(0),
      queue_([&]() { this->handleQueueEvent(); return false; }, queueSize)
{
    /* available connections */
    for (size_t i = 0; i < numParallel; i++) {
        HttpConnection * connPtr = new HttpConnection();
        shared_ptr<HttpConnection> connection(connPtr);
        connection->init(baseUrl);
        connection->onDone = [&, connPtr] (int result) {
            handleHttpConnectionDone(connPtr, result);
        };
        addSourceRightAway("connection" + to_string(i), connection);
        avlConnections_[i] = connPtr;
    }
    addSource("queue", queue_);
}

HttpClient::
~HttpClient()
{
    // cerr << "~HttpClient: " << this << "\n";
    shutdown();
}

void
HttpClient::
enablePipelining()
{
    throw ML::Exception("unimplemented");
    // ::curl_multi_setopt(handle_, CURLMOPT_PIPELINING, 1);
}

void
HttpClient::
debug(bool debugOn)
{
    debug_ = debugOn;
    MessageLoop::debug(debugOn);
}

bool
HttpClient::
enqueueRequest(const string & verb, const string & resource,
               const shared_ptr<HttpClientCallbacks> & callbacks,
               const MimeContent & content,
               const RestParams & queryParams, const RestParams & headers,
               int timeout)
{
    string url = baseUrl_ + resource + queryParams.uriEscaped();
    HttpRequest request(verb, url, callbacks, content, headers, timeout);

    return queue_.push_back(std::move(request));
}

void
HttpClient::
handleQueueEvent()
{
    size_t numConnections = avlConnections_.size() - nextAvail_;
    if (numConnections > 0) {
        /* "0" has a special meaning for pop_front and must be avoided here */
        auto requests = queue_.pop_front(numConnections);
        for (auto request: requests) {
            HttpConnection * conn = getConnection();
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
HttpClient::
handleHttpConnectionDone(HttpConnection * connection, int result)
{
    auto requests = queue_.pop_front(1);
    if (requests.size() > 0) {
        // cerr << "emptying queue...\n";
        connection->perform(move(requests[0]));
    }
    else {
        releaseConnection(connection);
    }
}

HttpConnection *
HttpClient::
getConnection()
{
    HttpConnection * conn;

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
HttpClient::
releaseConnection(HttpConnection * oldConnection)
{
    if (nextAvail_ > 0) {
        nextAvail_--;
        avlConnections_[nextAvail_] = oldConnection;
    }
}


/* HTTPCLIENTSIMPLECALLBACKS */

HttpClientSimpleCallbacks::
HttpClientSimpleCallbacks(const OnResponse & onResponse)
    : onResponse_(onResponse), statusCode_(0)
{
}

void
HttpClientSimpleCallbacks::
onResponseStart(const HttpRequest & rq,
                const string & httpVersion, int code)
{
    statusCode_ = code;
}

void
HttpClientSimpleCallbacks::
onHeader(const HttpRequest & rq, const char * data, size_t size)
{
    headers_.append(data, size);
}

void
HttpClientSimpleCallbacks::
onData(const HttpRequest & rq, const char * data, size_t size)
{
    body_.append(data, size);
}

void
HttpClientSimpleCallbacks::
onDone(const HttpRequest & rq, int error)
{
    onResponse(rq, error, statusCode_, move(headers_), move(body_));
}

void
HttpClientSimpleCallbacks::
onResponse(const HttpRequest & rq,
           int error, int status,
           string && headers, string && body)
{
    if (onResponse_) {
        onResponse_(rq, error, status, move(headers), move(body));
    }
    statusCode_ = 0;
    headers_.clear();
    body_.clear();
}
