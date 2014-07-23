/* tcp_socket.h                                                -*- C++ -*-
   Wolfgang Sourdeau, April 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   A helper base class for handling tcp sockets.
*/

#pragma once

#include <atomic>
#include <string>

#include "jml/arch/wakeup_fd.h"
#include "jml/utils/ring_buffer.h"

#include "async_writer_source.h"


/* TODO:
   An exception should trigger the closing of the socket and the returning of
   pending messages. */

namespace Datacratic {

struct Url;

/* CLIENT TCP SOCKET CONNECTION RESULT */

enum ConnectionResult {
    SUCCESS = 0,
    UNKNOWN_ERROR = 1,
    COULD_NOT_CONNECT = 2,
    HOST_UNKNOWN = 3,
    TIMEOUT = 4
};


/* CLIENT TCP SOCKET STATE */

enum ClientTcpSocketState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    DISCONNECTING
};


/* CLIENT TCP SOCKET */

struct ClientTcpSocket : public AsyncWriterSource
{
    typedef std::function<void(ConnectionResult, const std::vector<std::string> &)>
        OnConnectionResult;

    ClientTcpSocket(OnConnectionResult onConnectionResult = nullptr,
                    OnDisconnected onDisonnected = nullptr,
                    OnWriteResult onWriteResult = nullptr,
                    OnReceivedData onReceivedData = nullptr,
                    OnException onException = nullptr,
                    size_t maxMessages = 32,
                    size_t recvBufSize = 65536);

    virtual ~ClientTcpSocket();

    /* setup object */
    void init(const std::string & url);
    void init(const Url & url);
    void init(const std::string & address, int port);

    /* disable the Nagle algorithm (TCP_NODELAY) */
    void setUseNagle(bool useNagle);

    /* initiate or restore a connection to the target service */
    void connect();

    /* invoked when the status of the connection becomes available */
    virtual void onConnectionResult(ConnectionResult result,
                                    const std::vector<std::string> & msgs);

    /* state of the connection */
    ClientTcpSocketState state() const
    { return ClientTcpSocketState(state_); }

    void waitState(ClientTcpSocketState state) const;

private:
    void handleConnectionEvent(int socketFd, const ::epoll_event & event);
    void handleConnectionResult();

    std::string address_;
    int port_;
    int state_; /* ClientTcpSocketState */
    bool noNagle_;

    EpollCallback handleConnectionEventCb_;

    OnConnectionResult onConnectionResult_;
};

} // namespace Datacratic
