#include <errno.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>

#include <curlpp/cURLpp.hpp>
#include <curlpp/Easy.hpp>
#include <curlpp/Options.hpp>
#include <curlpp/Info.hpp>
#include <curlpp/Infos.hpp>

#include "jml/arch/cmp_xchg.h"
#include "jml/arch/timers.h"
#include "jml/arch/exception.h"
#include "jml/utils/guard.h"
#include "jml/utils/string_functions.h"

#include "soa/service/message_loop.h"
#include "soa/service/http_header.h"

#include "http_client.h"


using namespace std;
using namespace Datacratic;

namespace curlopt = curlpp::options;

namespace {

HttpClientError
translateError(CURLcode curlError)
{
    HttpClientError error;

    switch (curlError) {
    case CURLE_OK:
        error = HttpClientError::None;
        break;
    case CURLE_OPERATION_TIMEDOUT:
        error = HttpClientError::Timeout;
        break;
    case CURLE_COULDNT_RESOLVE_HOST:
        error = HttpClientError::HostNotFound;
        break;
    case CURLE_COULDNT_CONNECT:
        error = HttpClientError::CouldNotConnect;
        break;
    case CURLE_SEND_ERROR:
        error = HttpClientError::SendError;
        break;
    case CURLE_RECV_ERROR:
        error = HttpClientError::RecvError;
        break;
    default:
        ::fprintf(stderr, "returning 'unknown' for code %d\n", curlError);
        error = HttpClientError::Unknown;
    }

    return error;
}

}


/* HTTPCLIENTERROR */

std::ostream &
Datacratic::
operator << (std::ostream & stream, HttpClientError error)
{
    return stream << HttpClientCallbacks::errorMessage(error);
}



/* HTTPCLIENTCALLBACKS */

void
HttpClientCallbacks::
useDebug(const OnDebug& onDebug)
{
    onDebug_ = onDebug;
}

const string &
HttpClientCallbacks::
errorMessage(HttpClientError errorCode)
{
    static const string none = "No error";
    static const string unknown = "Unknown error";
    static const string hostNotFound = "Host not found";
    static const string couldNotConnect = "Could not connect";
    static const string timeout = "Request timed out";
    static const string sendError = "Failure sending network data";
    static const string recvError = "Failure receiving network data";

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
    case HttpClientError::SendError:
        return sendError;
    case HttpClientError::RecvError:
        return recvError;
    default:
        throw ML::Exception("invalid error code");
    };
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
onDone(const HttpRequest & rq, HttpClientError errorCode)
{
    if (onDone_)
        onDone_(rq, errorCode);
}

void
HttpClientCallbacks::
onDebug(const HttpRequest& rq, curl_infotype info, char *buffer, size_t size)
{
    if (onDebug_)
        onDebug_(rq, info, buffer, size);
}

/* HTTPCLIENT */

HttpClient::
HttpClient(const string & baseUrl, int numParallel)
    : AsyncEventSource(),
      noSSLChecks(false),
      baseUrl_(baseUrl),
      expect100Continue(true),
      fd_(-1),
      wakeup_(EFD_NONBLOCK | EFD_CLOEXEC),
      timerFd_(-1),
      connectionStash_(numParallel),
      avlConnections_(numParallel),
      nextAvail_(0)
{
    bool success(false);
    ML::Call_Guard guard([&] () {
        if (!success) { cleanupFds(); }
    });

    fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (fd_ == -1) {
        throw ML::Exception(errno, "epoll_create");
    }

    addFd(wakeup_.fd(), false, EPOLLIN);

    timerFd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    addFd(timerFd_, false, EPOLLIN);

    /* multi */
    ::CURLM ** handle = (::CURLM **) &multi_;
    handle_ = *handle;
    ::curl_multi_setopt(handle_, CURLMOPT_SOCKETFUNCTION, socketCallback);
    ::curl_multi_setopt(handle_, CURLMOPT_SOCKETDATA, this);
    ::curl_multi_setopt(handle_, CURLMOPT_TIMERFUNCTION, timerCallback);
    ::curl_multi_setopt(handle_, CURLMOPT_TIMERDATA, this);

    /* available connections */
    for (size_t i = 0; i < connectionStash_.size(); i++) {
        avlConnections_[i] = &connectionStash_[i];
    }

    /* kick start multi */
    int runningHandles;
    ::CURLMcode rc = ::curl_multi_socket_action(handle_,
                                                CURL_SOCKET_TIMEOUT, 0, 
                                                &runningHandles);
    if (rc != ::CURLM_OK) {
        throw ML::Exception("curl error " + to_string(rc));
    }

    success = true;
}

HttpClient::
HttpClient(HttpClient && other)
    noexcept
    : AsyncEventSource(move(other)),
      baseUrl_(move(other.baseUrl_)),
      expect100Continue(move(other.expect100Continue)),
      fd_(other.fd_),
      wakeup_(move(other.wakeup_)),
      timerFd_(other.timerFd_),
      connectionStash_(move(other.connectionStash_)),
      avlConnections_(move(other.avlConnections_)),
      nextAvail_(other.nextAvail_),
      queue_(move(other.queue_))
{
    other.fd_ = -1;
    other.timerFd_ = -1;

    /* the move operator of Curl::Multi is dubious but our multi handle is not
       supposed to be active at this point, therefore we do not need to move it */
    ::CURLM ** handle = (::CURLM **) &multi_;
    handle_ = *handle;
    ::curl_multi_setopt(handle_, CURLMOPT_SOCKETFUNCTION, socketCallback);
    ::curl_multi_setopt(handle_, CURLMOPT_SOCKETDATA, this);
    ::curl_multi_setopt(handle_, CURLMOPT_TIMERFUNCTION, timerCallback);
    ::curl_multi_setopt(handle_, CURLMOPT_TIMERDATA, this);
}

HttpClient::
~HttpClient()
{
    cleanupFds();
}

void
HttpClient::
sendExpect100Continue(bool value)
{
    expect100Continue = value;
}

void
HttpClient::
toggleTcpNoDelay(bool value)
{
    tcpNoDelay = value;
}

void
HttpClient::
enablePipelining()
{
    ::curl_multi_setopt(handle_, CURLMOPT_PIPELINING, 1);
}

HttpClient &
HttpClient::
operator = (HttpClient && other)
    noexcept
{
    AsyncEventSource::operator = (move(other));
    baseUrl_ = move(other.baseUrl_);
    expect100Continue = move(other.expect100Continue);
    fd_ = other.fd_;
    other.fd_ = -1;
    wakeup_ = move(other.wakeup_);
    timerFd_ = other.timerFd_;
    other.timerFd_ = -1;
    connectionStash_ = move(other.connectionStash_);
    avlConnections_ = move(other.avlConnections_);
    nextAvail_ = other.nextAvail_;
    queue_ = move(other.queue_);

    return *this;
}

void
HttpClient::
addFd(int fd, bool isMod, int flags)
    const
{
    // if (isMod) {
    //     cerr << "addFd: modding fd " + to_string(fd) + "\n";
    // }
    // else {
    //     cerr << "addFd: adding fd " + to_string(fd) + "\n";
    // }

    ::epoll_event event;

    ::memset(&event, 0, sizeof(event));

    event.events = flags;
    event.data.fd = fd;
    int rc = ::epoll_ctl(fd_, isMod ? EPOLL_CTL_MOD : EPOLL_CTL_ADD,
                         fd, &event);
    if (rc == -1) {
        rc = ::epoll_ctl(fd_, isMod ? EPOLL_CTL_ADD : EPOLL_CTL_MOD,
                         fd, &event);
    }
    if (rc == -1) {
	if (errno != EBADF) {
            throw ML::Exception(errno, "epoll_ctl");
        }
    }
}

void
HttpClient::
removeFd(int fd)
    const
{
    // cerr << "removeFd: removing fd " + to_string(fd) + "\n";
    ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr);
}

bool
HttpClient::
enqueueRequest(const string & verb, const string & resource,
               const shared_ptr<HttpClientCallbacks> & callbacks,
               const HttpRequest::Content & content,
               const RestParams & queryParams, const RestParams & headers,
               int timeout)
{
    string url = baseUrl_ + resource + queryParams.uriEscaped();
    {
        Guard guard(queueLock_);
        queue_.emplace(verb, url, callbacks, content, headers, timeout);
    }
    wakeup_.signal();

    return true;
}

std::vector<HttpRequest>
HttpClient::
popRequests(size_t number)
{
    std::vector<HttpRequest> requests;
    number = min(number, queue_.size());
    requests.reserve(number);

    {
        Guard guard(queueLock_);
        for (size_t i = 0; i < number; i++) {
            requests.emplace_back(move(queue_.front()));
            queue_.pop();
        }
    }

    return requests;
}

size_t
HttpClient::
queuedRequests() const {
    Guard guard(queueLock_);
    return queue_.size();
}

void
HttpClient::
cleanupFds()
    noexcept
{
    if (timerFd_ != -1) {
        ::close(timerFd_);
    }
    if (fd_ != -1) {
        ::close(fd_);
    }
}

int
HttpClient::
selectFd()
    const
{
    return fd_;
}

bool
HttpClient::
processOne()
{
    static const int nEvents(1024);
    ::epoll_event events[nEvents];

    while (true) {
        int res = ::epoll_wait(fd_, events, nEvents, 0);
        // ::fprintf(stderr, "processing %d events\n", res);
        if (res > 0) {
            for (int i = 0; i < res; i++) {
                handleEvent(events[i]);
            }
        }
        else if (res == 0) {
            break;
        }
        else if (res == -1) {
            if (errno == EINTR) {
                continue;
            }
            else {
                throw ML::Exception(errno, "epoll_wait");
            }
        }
    }

    return false;
}

void
HttpClient::
handleEvent(const ::epoll_event & event)
{
    if (event.data.fd == wakeup_.fd()) {
        handleWakeupEvent();
    }
    else if (event.data.fd == timerFd_) {
        handleTimerEvent();
    }
    else {
        handleMultiEvent(event);
    }
}

void
HttpClient::
handleWakeupEvent()
{
    // cerr << "  wakeup event\n";

    /* Deduplication of wakeup events */
    while (wakeup_.tryRead());

    size_t numAvail = avlConnections_.size() - nextAvail_;
    if (numAvail > 0) {
        vector<HttpRequest> requests = popRequests(numAvail);
        for (HttpRequest & request: requests) {
            HttpConnection *conn = getConnection();
            conn->request_ = move(request);
            conn->perform(noSSLChecks, expect100Continue, tcpNoDelay, debug_);
            multi_.add(&conn->easy_);
        }
    }
}

void
HttpClient::
handleTimerEvent()
{
    // cerr << "  timer event\n";
    uint64_t misses;
    ssize_t len = ::read(timerFd_, &misses, sizeof(misses));
    if (len == -1) {
        if (errno != EAGAIN) {
            throw ML::Exception(errno, "read timerd");
        }
    }
    int runningHandles;
    ::CURLMcode rc = ::curl_multi_socket_action(handle_,
                                                CURL_SOCKET_TIMEOUT, 0, 
                                                &runningHandles);
    if (rc != ::CURLM_OK) {
        throw ML::Exception("curl error " + to_string(rc));
    }
}

void
HttpClient::
handleMultiEvent(const ::epoll_event & event)
{
    // cerr << "  curl event\n";
    int actionFlags(0);
    if ((event.events & EPOLLIN) != 0) {
        actionFlags |= CURL_CSELECT_IN;
    }
    if ((event.events & EPOLLOUT) != 0) {
        actionFlags |= CURL_CSELECT_OUT;
    }
    
    int runningHandles;
    ::CURLMcode rc = ::curl_multi_socket_action(handle_, event.data.fd,
                                                actionFlags,
                                                &runningHandles);
    if (rc != ::CURLM_OK) {
        throw ML::Exception("curl error " + to_string(rc));
    }

    checkMultiInfos();
}

void
HttpClient::
checkMultiInfos()
{
    int remainingMsgs(0);
    CURLMsg * msg;
    // int count(0);
    while ((msg = curl_multi_info_read(handle_, &remainingMsgs))) {
        // count++;
        // cerr << to_string(count) << " msg\n";
        // cerr << "  remaining: " + to_string(remainingMsgs) << " msg\n";
        if (msg->msg == CURLMSG_DONE) {
            HttpConnection * conn(nullptr);
            ::curl_easy_getinfo(msg->easy_handle,
                                CURLINFO_PRIVATE, &conn);

            shared_ptr<HttpClientCallbacks> & cbs = conn->request_.callbacks_;
            cbs->onDone(conn->request_, translateError(msg->data.result));
            conn->clear();
            multi_.remove(&conn->easy_);
            releaseConnection(conn);
            wakeup_.signal();
            // cerr << "* request done\n";
        }
        else {
            cerr << "? not done\n";
        }
    }
}

int
HttpClient::
socketCallback(CURL *e, curl_socket_t s, int what, void *clientP, void *sockp)
{
    HttpClient *this_ = static_cast<HttpClient *>(clientP);

    return this_->onCurlSocketEvent(e, s, what, sockp);
}

int
HttpClient::
onCurlSocketEvent(CURL *e, curl_socket_t fd, int what, void *sockp)
{
    // cerr << "onCurlSocketEvent: " + to_string(fd) + " what: " + to_string(what) + "\n";

    if (what == CURL_POLL_REMOVE) {
        // cerr << "remove fd\n";
        ::curl_multi_assign(handle_, fd, nullptr);
        removeFd(fd);
    }
    else {
        int flags(0);
        if ((what & CURL_POLL_IN)) {
            flags |= EPOLLIN;
        }
        if ((what & CURL_POLL_OUT)) {
            flags |= EPOLLOUT;
        }
        addFd(fd, (sockp != nullptr), flags);
        if (sockp == nullptr) {
            ::curl_multi_assign(handle_, fd, this);
        }
    }

    return 0;
}

int
HttpClient::
timerCallback(CURLM *multi, long timeoutMs, void *clientP)
{
    HttpClient *this_ = static_cast<HttpClient *>(clientP);

    return this_->onCurlTimerEvent(timeoutMs);
}

int
HttpClient::
onCurlTimerEvent(long timeoutMs)
{
    // cerr << "onCurlTimerEvent: timeout = " + to_string(timeoutMs) + "\n";

    struct itimerspec timespec;
    memset(&timespec, 0, sizeof(timespec));
    if (timeoutMs > 0) {
        timespec.it_value.tv_sec = timeoutMs / 1000;
        timespec.it_value.tv_nsec = (timeoutMs % 1000) * 1000000;
    }
    int res = ::timerfd_settime(timerFd_, 0, &timespec, nullptr);
    if (res == -1) {
        throw ML::Exception(errno, "timerfd_settime");
    }

    if (timeoutMs < 1) {
        // cerr << "* doing timeout\n";
        int runningHandles;
        ::CURLMcode rc = ::curl_multi_socket_action(handle_,
                                                    CURL_SOCKET_TIMEOUT, 0, 
                                                    &runningHandles);
        if (rc != ::CURLM_OK) {
            throw ML::Exception("curl error " + to_string(rc));
        }
        checkMultiInfos();
    }

    return 0;
}

HttpClient::
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


/* HTTPCLIENT::HTTPCONNECTION */

HttpClient::
HttpConnection::
HttpConnection()
    : onHeader_([&] (const char * data, size_t ofs1, size_t ofs2) {
          return this->onCurlHeader(data, ofs1 * ofs2);
      }),
      onWrite_([&] (const char * data, size_t ofs1, size_t ofs2) {
          return this->onCurlWrite(data, ofs1 * ofs2);
      }),
      onRead_([&] (char * data, size_t ofs1, size_t ofs2) {
          return this->onCurlRead(data, ofs1 * ofs2);
      }),
      onDebug_([&] (curl_infotype info, char *buffer, size_t size) {
          return this->onCurlDebug(info, buffer, size);
      }),
      afterContinue_(false), uploadOffset_(0)
{
}

void
HttpClient::
HttpConnection::
perform(bool noSSLChecks, bool withExpect100Continue, bool tcpNoDelay, bool debug)
{
    // cerr << "* performRequest\n";

    // cerr << "nbrRequests: " + to_string(nbrRequests_) + "\n";

    afterContinue_ = false;

    easy_.reset();
    easy_.setOpt<curlopt::Url>(request_.url_);
    // easy_.setOpt<curlopt::CustomRequest>(request_.verb_);

    list<string> curlHeaders;
    for (const auto & it: request_.headers_) {
        curlHeaders.push_back(it.first + ": " + it.second);
    }
    if (request_.verb_ != "GET") {
        const string & data = request_.content_.str;
        if (request_.verb_ == "PUT") {
            easy_.setOpt<curlopt::Upload>(true);
            easy_.setOpt<curlopt::InfileSize>(data.size());
        }
        else if (request_.verb_ == "POST") {
            easy_.setOpt<curlopt::Post>(true);
            easy_.setOpt<curlopt::PostFields>(data);
            easy_.setOpt<curlopt::PostFieldSize>(data.size());
        }
        else if (request_.verb_ == "HEAD") {
            easy_.setOpt<curlopt::NoBody>(true);
        }
        curlHeaders.push_back("Content-Length: "
                              + to_string(data.size()));
        curlHeaders.push_back("Transfer-Encoding:");
        curlHeaders.push_back("Content-Type: "
                              + request_.content_.contentType);

        if (!withExpect100Continue) {
            curlHeaders.push_back("Expect:");
        }
    }
    easy_.setOpt<curlopt::HttpHeader>(curlHeaders);

    easy_.setOpt<curlopt::CustomRequest>(request_.verb_);
    easy_.setOpt<curlopt::Private>(this);
    easy_.setOpt<curlopt::HeaderFunction>(onHeader_);
    easy_.setOpt<curlopt::WriteFunction>(onWrite_);
    easy_.setOpt<curlopt::ReadFunction>(onRead_);
    easy_.setOpt<curlopt::DebugFunction>(onDebug_);
    easy_.setOpt<curlopt::BufferSize>(65536);
    if (request_.timeout_ != -1) {
        easy_.setOpt<curlopt::Timeout>(request_.timeout_);
    }
    easy_.setOpt<curlopt::NoSignal>(true);
    easy_.setOpt<curlopt::NoProgress>(true);
    if (noSSLChecks) {
        easy_.setOpt<curlopt::SslVerifyHost>(false);
        easy_.setOpt<curlopt::SslVerifyPeer>(false);
    }
    if (debug) {
        easy_.setOpt<curlopt::Verbose>(1L);
    }
    if (tcpNoDelay) {
        easy_.setOpt<curlopt::TcpNoDelay>(true);
    }
}

size_t
HttpClient::
HttpConnection::
onCurlHeader(const char * data, size_t size)
    noexcept
{
    // cerr << "onCurlHeader\n";
    string headerLine(data, size);
    if (headerLine.find("HTTP/1.1 100") == 0) {
        afterContinue_ = true;
    }
    else if (afterContinue_) {
        if (headerLine == "\r\n")
            afterContinue_ = false;
    }
    else {
        if (headerLine.find("HTTP/") == 0) {
            size_t lineSize = headerLine.size();
            size_t oldTokenIdx(0);
            size_t tokenIdx = headerLine.find(" ");
            if (tokenIdx == string::npos || tokenIdx >= lineSize) {
                throw ML::Exception("malformed header");
            }
            string version = headerLine.substr(oldTokenIdx, tokenIdx);

            oldTokenIdx = tokenIdx + 1;
            tokenIdx = headerLine.find(" ", oldTokenIdx);
            if (tokenIdx == string::npos || tokenIdx >= lineSize) {
                throw ML::Exception("malformed header");
            }
            int code = stoi(headerLine.substr(oldTokenIdx, tokenIdx));

            request_.callbacks_->onResponseStart(request_,
                                                 move(version), code);
        }
        else {
            request_.callbacks_->onHeader(request_, data, size);
        }
    }

    return size;
}

size_t
HttpClient::
HttpConnection::
onCurlWrite(const char * data, size_t size)
    noexcept
{
    // cerr << "onCurlWrite\n";
    request_.callbacks_->onData(request_, data, size);
    return size;
}

size_t
HttpClient::
HttpConnection::
onCurlRead(char * buffer, size_t bufferSize)
    noexcept
{
    const string & data = request_.content_.str;
    size_t chunkSize = data.size() - uploadOffset_;
    if (chunkSize > bufferSize) {
        chunkSize = bufferSize;
    }
    const char * chunkStart = data.c_str() + uploadOffset_;
    copy(chunkStart, chunkStart + chunkSize, buffer);
    uploadOffset_ += chunkSize;

    return chunkSize;
}

int
HttpClient::
HttpConnection::
onCurlDebug(curl_infotype info, char *buffer, size_t size)
{
    request_.callbacks_->onDebug(request_, info, buffer, size);
    return 0;
}

/* HTTPCLIENTSIMPLECALLBACKS */

HttpClientSimpleCallbacks::
HttpClientSimpleCallbacks(const OnResponse & onResponse)
    : onResponse_(onResponse)
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
onDone(const HttpRequest & rq, HttpClientError error)
{
    onResponse(rq, error, statusCode_, move(headers_), move(body_));
}

void
HttpClientSimpleCallbacks::
onResponse(const HttpRequest & rq,
           HttpClientError error, int status,
           string && headers, string && body)
{
    if (onResponse_) {
        onResponse_(rq, error, status, move(headers), move(body));
    }
}
