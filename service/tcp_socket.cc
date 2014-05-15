/* tcp_socket.cc
   Wolfgang Sourdeau, April 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   A helper base class for handling tcp connections.
*/

#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "googleurl/src/gurl.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/guard.h"
#include "soa/types/url.h"

#include "tcp_socket.h"

using namespace std;
using namespace Datacratic;

ClientTcpSocket::
ClientTcpSocket(OnConnectionResult onConnectionResult,
                OnDisconnected onDisconnected,
                OnWriteResult onWriteResult,
                OnReceivedData onReceivedData,
                OnException onException,
                size_t maxMessages,
                size_t recvBufSize)
    : AsyncEventSource(),
      port_(-1),
      epollFd_(-1),
      socket_(-1),
      noNagle_(false),
      recvBufSize_(recvBufSize),
      writeReady_(false),
      wakeup_(EFD_NONBLOCK | EFD_CLOEXEC),
      threadBuffer_(maxMessages),
      currentSent_(0),
      bytesSent_(0),
      remainingMsgs_(0),
      state_(ClientTcpSocketState::DISCONNECTED),
      onConnectionResult_(onConnectionResult),
      onDisconnected_(onDisconnected),
      onReceivedData_(onReceivedData),
      onException_(onException)
{
    epollFd_ = ::epoll_create(2);
    if (epollFd_ == -1)
        throw ML::Exception(errno, "epoll_create");

    handleWakeupEventCb_ = [&] (const struct epoll_event & event) {
        this->handleWakeupEvent(event);
    };
    addFdOneShot(wakeup_.fd(), handleWakeupEventCb_, true, false);
}

ClientTcpSocket::
~ClientTcpSocket()
{
    // cerr << "~ClientTcpSocket\n";
    if (socket_ != -1) {
        // cerr << "closing fd: " + to_string(socket_) + "\n";
        closeFd();
    }
    closeEpollFd();
}

void
ClientTcpSocket::
init(const string & url)
{
    init(Url(url));
}

void
ClientTcpSocket::
init(const Url & url)
{
    int port = url.url->EffectiveIntPort();
    init(url.host(), port);
}

void
ClientTcpSocket::
init(const string & address, int port)
{
    if (state_ == ClientTcpSocketState::CONNECTING
        || state_ == ClientTcpSocketState::CONNECTED) {
        throw ML::Exception("connection already pending or established");
    }
    if (address.empty()) {
        throw ML::Exception("invalid address: " + address);
    }
    if (port < 1) {
        throw ML::Exception("invalid port: " + to_string(port));
    }
    address_ = address;
    port_ = port;
}

void
ClientTcpSocket::
waitState(ClientTcpSocketState state)
    const
{
    while (state_ != state) {
        int oldState = state_;
        ML::futex_wait(state_, oldState);
    }
}

void
ClientTcpSocket::
setUseNagle(bool useNagle)
{
    if (socket_ != -1) {
        throw ML::Exception("socket already created");
    }

    noNagle_ = !useNagle;
}

void
ClientTcpSocket::
connect()
{
    // cerr << "connect...\n";
    ExcCheck(socket_ == -1, "socket is not closed");
    ExcCheck(!address_.empty(), "no address set");
    ExcCheck(!canSendMessages(),
             "connection already pending or established");

    state_ = ClientTcpSocketState::CONNECTING;
    ML::futex_wake(state_);

    bool success(false);
    int res;

    res = ::socket(AF_INET,
                   SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (res == -1) {
        throw ML::Exception(errno, "socket");
    }
    socket_ = res;
    // cerr << "socket created\n";

    auto cleanup = [&] () {
        if (!success) {
            ::close(socket_);
            socket_ = -1;
            state_ = ClientTcpSocketState::DISCONNECTED;
            ML::futex_wake(state_);
        }
    };
    ML::Call_Guard guard(cleanup);

    if (noNagle_) {
        int flag = 1;
        res = setsockopt(socket_,
                         IPPROTO_TCP, TCP_NODELAY,
                         (char *) &flag, sizeof(int));
        if (res == -1) {
            throw ML::Exception(errno, "setsockopt TCP_NODELAY");
        }
    }

    struct sockaddr_in addr;
    addr.sin_port = htons(port_);
    addr.sin_family = AF_INET;

    // cerr << " connecting to host: " + address_ + "\n";
    res = ::inet_aton(address_.c_str(), &addr.sin_addr);
    if (res == 0) {
        // cerr << "host is not an ip\n";
        struct hostent hostentry;
        struct hostent * hostentryP;
        int hErrnoP;

        char buffer[1024];
        res = gethostbyname_r(address_.c_str(),
                              &hostentry,
                              buffer, sizeof(buffer),
                              &hostentryP, &hErrnoP);
        if (res == -1 || hostentry.h_addr_list == nullptr) {
            cerr << "host is not valid\n";
            onConnectionResult(ConnectionResult::HOST_UNKNOWN, {});
            return;
        }
        addr.sin_family = hostentry.h_addrtype;
        addr.sin_addr.s_addr = *(in_addr_t *) hostentry.h_addr_list[0];
    }

    res = ::connect(socket_,
                    (const struct sockaddr *) &addr, sizeof(sockaddr_in));
    if (res == -1) {
        if (errno != EINPROGRESS) {
            onConnectionResult(ConnectionResult::COULD_NOT_CONNECT,
                               {});
            return;
        }
        // cerr << "connection in progress\n";
    }
    else {
        // cerr << "connection established\n";
        onConnectionResult(ConnectionResult::SUCCESS, {});
        state_ = ClientTcpSocketState::CONNECTED;
        ML::futex_wake(state_);
    }

    success = true;
    handleSocketEventCb_ = [&] (const struct epoll_event & event) {
        this->handleSocketEvent(event);
    };
    addFdOneShot(socket_, handleSocketEventCb_,
                 state_ == ClientTcpSocketState::CONNECTED,
                 true);
}

void
ClientTcpSocket::
onConnectionResult(ConnectionResult result, const vector<string> & msgs)
{
    if (onConnectionResult_) {
        onConnectionResult_(result, msgs);
    }
}

void
ClientTcpSocket::
onDisconnected(bool fromPeer)
{
    if (onDisconnected_) {
        onDisconnected_(fromPeer);
    }
}

void
ClientTcpSocket::
onWriteResult(int error,
              const string & written, size_t writtenSize)
{
    if (onWriteResult_) {
        onWriteResult_(error, written, writtenSize);
    }
}

void
ClientTcpSocket::
onReceivedData(const char * buffer, size_t bufferSize)
{
    if (onReceivedData_) {
        onReceivedData_(buffer, bufferSize);
    }
}

void
ClientTcpSocket::
onException(const exception_ptr & excPtr)
{
    if (onException_) {
        onException(excPtr);
    }
}

bool
ClientTcpSocket::
canSendMessages()
    const
{
    return (state_ == ClientTcpSocketState::CONNECTED
            || state_ == ClientTcpSocketState::CONNECTING);
}

bool
ClientTcpSocket::
write(const string & data)
{
    return write(data.c_str(), data.size());
}

bool
ClientTcpSocket::
write(const char * data, size_t size)
{
    return write(string(data, size));
}

bool
ClientTcpSocket::
write(string && data)
{
    bool result(true);

    if (canSendMessages()) {
        if (threadBuffer_.tryPush(move(data))) {
            wakeup_.signal();
            remainingMsgs_++;
        }
        else {
            result = false;
        }
    }
    else {
        throw ML::Exception("cannot write while not connected");
    }

    return result;
}

void
ClientTcpSocket::
requestClose()
{
    if (canSendMessages()) {
        state_ = ClientTcpSocketState::DISCONNECTING;
        ML::futex_wake(state_);
        wakeup_.signal();
    }
    else {
        cerr << "already disconnected/ing\n";
    }
}

/* async event source */
bool
ClientTcpSocket::
processOne()
{
    static const int nEvents(2);
    struct epoll_event events[nEvents];

    // cerr << "sizeof(events): " + to_string() + "\n";
    try {
        int res = epoll_wait(epollFd_, events, nEvents, 0);
        if (res == -1) {
            throw ML::Exception(errno, "epoll_wait");
        }

        for (int i = 0; i < res; i++) {
            auto * fn = static_cast<EpollCallback *>(events[i].data.ptr);
            (*fn)(events[i]);
        }
    }
    catch (...) {
        handleException();
    }

    return false;
}

void
ClientTcpSocket::
closeEpollFd()
{
    if (epollFd_ == -1) 
        return;

    ::close(epollFd_);
    epollFd_ = -1;
}

/* epoll operations */

void
ClientTcpSocket::
performAddFd(int fd, EpollCallback & cb, bool readerFd, bool writerFd,
             bool restart)
{
    if (epollFd_ == -1)
        return;
    //cerr << Date::now().print(4) << "restarted " << fd << " one-shot" << endl;

    struct epoll_event event;
    event.events = EPOLLONESHOT;
    if (readerFd) {
        event.events |= EPOLLIN;
    }
    if (writerFd) {
        event.events |= EPOLLOUT;
    }
    event.data.ptr = &cb;

    int operation = restart ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    int res = epoll_ctl(epollFd_, operation, fd, &event);
    // cerr << (string("epoll_ctl:")
    //          + " restart=" + to_string(restart)
    //          + " fd=" + to_string(fd)
    //          + " readerFd=" + to_string(readerFd)
    //          + " writerFd=" + to_string(writerFd)
    //          + "\n");
    if (res == -1) {
        string message = (string("epoll_ctl:")
                          + " restart=" + to_string(restart)
                          + " fd=" + to_string(fd)
                          + " readerFd=" + to_string(readerFd)
                          + " writerFd=" + to_string(writerFd));
        throw ML::Exception(errno, message);
    }
}

void
ClientTcpSocket::
removeFd(int fd)
{
    if (epollFd_ == -1)
        return;
    //cerr << Date::now().print(4) << "removed " << fd << endl;

    int res = epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, 0);
    if (res == -1)
        throw ML::Exception(errno, "epoll_ctl DEL " + to_string(fd));
}

/* wakeup events */

void
ClientTcpSocket::
handleWakeupEvent(const struct epoll_event & event)
{
    if ((event.events & EPOLLIN) != 0) {
        eventfd_t val;
        wakeup_.tryRead(val);
        restartFdOneShot(wakeup_.fd(), handleWakeupEventCb_, true, false);

        if (writeReady_) {
            // cerr << "flush from wakeup\n";
            flush();
        }

        if (state_ == ClientTcpSocketState::DISCONNECTING) {
            if (remainingMsgs_ > 0 || currentLine_.size() > 0) {
                // cerr << "postponing disconnection\n";
                wakeup_.signal();
            }
            else {
                // cerr << "immediate disconnection\n";
                closeFd();
            }
        }
    }
    else {
        throw ML::Exception("unhandled event");
    }
}

void
ClientTcpSocket::
flush()
{
    if (!writeReady_) {
        cerr << "BAD: not ready for writing\n";
    }

    auto popLine = [&] {
        bool result;

        // cerr << "fetching line\n";
        if (threadBuffer_.tryPop(currentLine_)) {
            ExcAssert(remainingMsgs_ != 0);
            remainingMsgs_--;
            currentSent_ = 0;
            result = true;
        }
        else {
            // cerr << "no line fetched\n";
            result = false;
        }

        return result;
    };

    // cerr << "flush1\n";
    if (currentLine_.size() == 0) {
        if (!popLine()) {
            return;
        }
    }
    // else {
    //     cerr << "has current line\n";
    // }

    bool done(false);
    ssize_t remaining(currentLine_.size() - currentSent_);
    // cerr << "initial remaining: " + to_string(remaining) + " bytes\n";
    // cerr << "initial curentLine size: " + to_string(currentLine_.size()) + " bytes\n";
    // cerr << "initial currentSent_: " + to_string(currentSent_) + " bytes\n";

    errno = 0;

    while (writeReady_ && !done) {
        const char * data = currentLine_.c_str() + currentSent_;
        // cerr << " sending " << to_string(remaining) + " bytes\n";
        ssize_t len = ::write(socket_, data, remaining);
        // ::fprintf(stderr, "write result: %ld, initial remaining: %ld,"
        //           "  errno: %d\n", len, remaining, errno);
        if (len > 0) {
            currentSent_ += len;
            remaining -= len;
            bytesSent_ += len;
            if (remaining == 0) {
                handleWriteResult(0, currentLine_, currentLine_.size());
                if (popLine()) {
                    data = currentLine_.c_str();
                    remaining = currentLine_.size();
                }
                else {
                    currentLine_.clear();
                    done = true;
                }
            }
        }
        else {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                writeReady_ = false;
            }
            else {
                handleWriteResult(errno, currentLine_, currentSent_);
                currentLine_.clear();
                writeReady_ = false;
                if (errno == EPIPE || errno == EBADF) {
                    handleDisconnection(true);
                }
                else {
                    /* This exception indicates a lack of code in the
                       handling of errno. In a perfect world, it should
                       never ever be thrown. */
                    throw ML::Exception(errno, "unhandled write error");
                }
            }
        }
    }

    // cerr << "flush end with writeReady = "  + to_string(writeReady_) + "\n";
}

void
ClientTcpSocket::
closeFd()
{
    // cerr << "closeFd...\n";
    ExcCheck(!threadBuffer_.couldPop(),
             "message queue not empty");
    ExcCheck(state_ != ClientTcpSocketState::DISCONNECTED,
             "already closed (state)");
    ExcCheck(socket_ != -1, "already closed (socket)");

    if (state_ != ClientTcpSocketState::DISCONNECTING) {
        state_ = ClientTcpSocketState::DISCONNECTING;
        ML::futex_wake(state_);
    }

    if (socket_ != -1) {
        try {
            removeFd(socket_);
        }
        catch(const ML::Exception & exc)
        {}
        ::shutdown(socket_, SHUT_RDWR);
        ::close(socket_);
        // cerr << "socket " + to_string(socket_) + " now closed\n";
        handleDisconnection(false);
        // ::sleep(3.0);
    }
}

/* fd events */

void
ClientTcpSocket::
handleSocketEvent(const struct epoll_event & event)
{
    // cerr << "handleSocketEvent\n";
    if ((event.events & EPOLLOUT) != 0) {
        // cerr << "  handleWriteReady\n";
        handleWriteReady();
    }
    if ((event.events & EPOLLIN) != 0) {
        // cerr << "  handleReadReady\n";
        handleReadReady();
    }
    if ((event.events & EPOLLHUP) != 0) {
        // cerr << "  handleDisconnection\n";
        handleDisconnection(true);
    }
    else {
        if (state_ != ClientTcpSocketState::DISCONNECTED) {
            restartFdOneShot(socket_, handleSocketEventCb_,
                             state_ == ClientTcpSocketState::CONNECTED,
                             !writeReady_);
        }
    }
}

void
ClientTcpSocket::
handleConnectionResult()
{
    int32_t result;
    socklen_t len(sizeof(result));

    // cerr << "handle connection result\n";

    int res = getsockopt(socket_, SOL_SOCKET, SO_ERROR,
                         (void *) &result, &len);
    if (res == -1) {
        throw ML::Exception(errno, "getsockopt");
    }

    ConnectionResult connResult;
    vector<string> lostMessages;
    if (result == 0) {
        connResult = SUCCESS;
        // cerr << "connection successful\n";
        state_ = ClientTcpSocketState::CONNECTED;
    }
    else if (result == ENETUNREACH) {
        connResult = HOST_UNKNOWN;
    }
    else if (result == ECONNREFUSED
             || result == EHOSTDOWN
             || result == EHOSTUNREACH) {
        connResult = COULD_NOT_CONNECT;
    }
    else {
        throw ML::Exception("unhandled error:" + to_string(result));
    }

    if (connResult == SUCCESS) {
        errno = 0;
    }
    else {
        removeFd(socket_);
        ::close(socket_);
        socket_ = -1;
        state_ = ClientTcpSocketState::DISCONNECTED;
        string lostMessage;
        while (threadBuffer_.tryPop(lostMessage)) {
            lostMessages.emplace_back(move(lostMessage));
        }
        remainingMsgs_ = 0;
    }
    ML::futex_wake(state_);
    onConnectionResult(connResult, lostMessages);
}

void
ClientTcpSocket::
handleDisconnection(bool fromPeer)
{
    if (state_ != ClientTcpSocketState::DISCONNECTED) {
        if (fromPeer) {
            removeFd(socket_);
            ::close(socket_);
        }
        socket_ = -1;
        writeReady_ = false;
        state_ = ClientTcpSocketState::DISCONNECTED;
        ML::futex_wake(state_);
        onDisconnected(fromPeer);
    }
}

void
ClientTcpSocket::
handleReadReady()
{
    char buffer[recvBufSize_];

    // cerr << "handleReadReady\n";
    errno = 0;
    while (1) {
        ssize_t s = ::read(socket_, buffer, recvBufSize_);
        // ::fprintf(stderr, "read result: %ld, errno: %d\n", s, errno);
        if (s > 0) {
            onReceivedData(buffer, s);
        }
        else {
            if (errno == EWOULDBLOCK) {
                // cerr << "done reading\n";
                break;
            }
            else if (errno == EBADF || errno == EINVAL) {
                // cerr << "badf\n";
                break;
            }
            if (s == -1) {
                throw ML::Exception(errno, "read");
            }
            else {
                break;
            }
        }
    }
}

void
ClientTcpSocket::
handleWriteReady()
{
    if (state_ == ClientTcpSocketState::CONNECTING) {
        handleConnectionResult();
    }
    if (state_ != ClientTcpSocketState::DISCONNECTED) {
        writeReady_ = true;
        // cerr << "flush from write ready\n";
        flush();
    }
}

void
ClientTcpSocket::
handleWriteResult(int error,
                  const string & written, size_t writtenSize)
{
    onWriteResult(error, written, writtenSize);
}

void
ClientTcpSocket::
handleException()
{
    onException(current_exception());
}
