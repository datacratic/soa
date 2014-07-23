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
    : AsyncWriterSource(onDisconnected, onWriteResult, onReceivedData,
                        onException, maxMessages, recvBufSize),
      port_(-1),
      state_(ClientTcpSocketState::DISCONNECTED),
      noNagle_(false),
      onConnectionResult_(onConnectionResult)
{
}

ClientTcpSocket::
~ClientTcpSocket()
{
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
    if (state() != DISCONNECTED) {
        throw ML::Exception("socket already created");
    }

    noNagle_ = !useNagle;
}

void
ClientTcpSocket::
connect()
{
    // cerr << "connect...\n";
    ExcCheck(state() == DISCONNECTED, "socket is not closed");
    ExcCheck(!address_.empty(), "no address set");
    ExcCheck(!canSendMessages(),
             "connection already pending or established");

    state_ = ClientTcpSocketState::CONNECTING;
    ML::futex_wake(state_);

    int res = ::socket(AF_INET,
                       SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (res == -1) {
        state_ = ClientTcpSocketState::DISCONNECTED;
        ML::futex_wake(state_);
        throw ML::Exception(errno, "socket");
    }

    int socketFd = res;
    // cerr << "socket created\n";

    /* cleanup */
    bool success(false);
    auto cleanup = [&] () {
        if (!success) {
            ::close(socketFd);
            state_ = ClientTcpSocketState::DISCONNECTED;
            ML::futex_wake(state_);
        }
    };
    ML::Call_Guard guard(cleanup);

    /* nagle */
    if (noNagle_) {
        int flag = 1;
        res = setsockopt(socketFd,
                         IPPROTO_TCP, TCP_NODELAY,
                         (char *) &flag, sizeof(int));
        if (res == -1) {
            throw ML::Exception(errno, "setsockopt TCP_NODELAY");
        }
    }

    /* address resolution */
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

    /* connection */
    res = ::connect(socketFd,
                    (const struct sockaddr *) &addr, sizeof(sockaddr_in));
    if (res == -1) {
        if (errno != EINPROGRESS) {
            onConnectionResult(ConnectionResult::COULD_NOT_CONNECT,
                               {});
            return;
        }
        handleConnectionEventCb_
            = [&, socketFd] (const ::epoll_event & event) {
            this->handleConnectionEvent(socketFd, event);
        };
        addFdOneShot(socketFd, handleConnectionEventCb_, false, true);
        // cerr << "connection in progress\n";
    }
    else {
        // cerr << "connection established\n";
        setFd(socketFd);
        onConnectionResult(ConnectionResult::SUCCESS, {});
        state_ = ClientTcpSocketState::CONNECTED;
        ML::futex_wake(state_);
    }

    /* no cleanup required */
    success = true;
}

void
ClientTcpSocket::
handleConnectionEvent(int socketFd, const ::epoll_event & event)
{
    // cerr << "handle connection result\n";
    int32_t result;
    socklen_t len(sizeof(result));
    int res = getsockopt(socketFd, SOL_SOCKET, SO_ERROR,
                         (void *) &result, &len);
    if (res == -1) {
        throw ML::Exception(errno, "getsockopt");
    }

    ConnectionResult connResult;
    if (result == 0) {
        connResult = SUCCESS;
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

    vector<string> lostMessages;
    removeFd(socketFd);
    if (connResult == SUCCESS) {
        errno = 0;
        setFd(socketFd);
        // cerr << "connection successful\n";
        state_ = ClientTcpSocketState::CONNECTED;
    }
    else {
        ::close(socketFd);
        state_ = ClientTcpSocketState::DISCONNECTED;
        lostMessages = emptyMessageQueue();
    }
    ML::futex_wake(state_);
    onConnectionResult(connResult, lostMessages);
}

void
ClientTcpSocket::
onConnectionResult(ConnectionResult result, const vector<string> & msgs)
{
    if (onConnectionResult_) {
        onConnectionResult_(result, msgs);
    }
}
