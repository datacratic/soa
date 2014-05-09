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

#include "async_event_source.h"


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

struct ClientTcpSocket : public AsyncEventSource
{
    typedef std::function<void(ConnectionResult, const std::vector<std::string> &)>
        OnConnectionResult;
    typedef std::function<void()> OnDisconnected;
    typedef std::function<void(int error,
                               const std::string & written,
                               size_t writtenSize)> OnWriteResult;
    typedef std::function<void(const char *, size_t)> OnReceivedData;
    typedef std::function<void(const std::exception_ptr &)> OnException;

    ClientTcpSocket(OnConnectionResult onConnectionResult = nullptr,
                    OnDisconnected onDisonnected = nullptr,
                    OnWriteResult onWriteResult = nullptr,
                    OnReceivedData onReceivedData = nullptr,
                    OnException onException = nullptr,
                    size_t maxMessages = 32,
                    size_t recvBufSize = 16284);

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

    /* enqueue "data" for sending to the service, once the socket becomes
       available for writing */
    bool write(const std::string & data);
    bool write(const char * data, size_t size);
    bool write(std::string && data);

    /* invoked when a write operation has been performed, where "written" is
       the string that was sent "writtenSize" is the amount of bytes that was
       sent. The latter is always equal to the length of the string with
       error is 0. */
    virtual void onWriteResult(int error,
                               const std::string & written,
                               size_t writtenSize);

    /* close the connection as soon as all bytes have been sent and
     * received */
    void requestClose();

    /* invoked when the connection is closed */
    virtual void onDisconnected();

    /* invoked when the data is available for reading */
    virtual void onReceivedData(const char * data, size_t size);

    /* invoked when an exception occurs during the handling of events */
    virtual void onException(const std::exception_ptr & excPtr);

    /* state of the connection */
    ClientTcpSocketState state() const
    { return ClientTcpSocketState(state_); }

    /* we are ready to accept messages for sending */
    bool canSendMessages() const;

    void waitState(ClientTcpSocketState state) const;

    /* number of bytes actually sent */
    size_t bytesSent() const
    { return bytesSent_; }

    /* AsyncEventSource interface */
    virtual int selectFd() const
    { return epollFd_; }
    virtual bool processOne();

private:
    void doClose();

    typedef std::function<void (struct epoll_event &)> EpollCallback;

    void addFdOneShot(int fd, EpollCallback & cb, bool writerFd = false);
    void restartFdOneShot(int fd, EpollCallback & cb, bool writerFd = false);
    void removeFd(int fd);
    void close();

    void handleSocketEvent(const struct epoll_event & event);
    void handleConnectionResult();
    void handleDisconnection();
    void handleReadReady();
    void handleWriteReady();
    void handleWriteResult(int error,
                           const std::string & written, size_t writtenSize);
    void handleException();

    void handleWakeupEvent(const struct epoll_event & event);
    EpollCallback handleSocketEventCb_;
    EpollCallback handleWakeupEventCb_;

    void flush();

    std::string address_;
    int port_;

    int epollFd_;
    int socket_;

    bool noNagle_;

    size_t recvBufSize_;

    bool writeReady_;

    ML::Wakeup_Fd wakeup_;
    ML::RingBufferSRMW<std::string> threadBuffer_;
    std::string currentLine_;
    size_t currentSent_;

    size_t bytesSent_;
    std::atomic<size_t> remainingMsgs_;
    int state_; /* ClientTcpSocketState */

    OnConnectionResult onConnectionResult_;
    OnDisconnected onDisconnected_;
    OnWriteResult onWriteResult_;
    OnReceivedData onReceivedData_;
    OnException onException_;

    /* future parameters:
       - writeMany: send multiple lines at once
    */
};

} // namespace Datacratic
