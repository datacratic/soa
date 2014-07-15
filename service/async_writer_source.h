/* async_writer_source.h                                           -*- C++ -*-
   Wolfgang Sourdeau, April 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   A base class for handling writable file descriptors.
*/

#include <sys/epoll.h>

#include <atomic>
#include <string>

#include "jml/arch/wakeup_fd.h"
#include "jml/utils/ring_buffer.h"

#include "async_event_source.h"


namespace Datacratic {

/* ASYNC WRITER SOURCE */

/* A base class enabling the asynchronous and buffered writing of data to a
 * file descriptor. */

struct AsyncWriterSource : public AsyncEventSource
{
    typedef std::function<void(bool,
                               const std::vector<std::string> & msgs)> OnDisconnected;
    typedef std::function<void(int error,
                               const std::string & written,
                               size_t writtenSize)> OnWriteResult;
    typedef std::function<void(const char *, size_t)> OnReceivedData;
    typedef std::function<void(const std::exception_ptr &)> OnException;

    AsyncWriterSource(const OnDisconnected & onDisconnected,
                      const OnWriteResult & onWriteResult,
                      const OnReceivedData & onReceivedData,
                      const OnException & onException,
                      size_t maxMessages,
                      size_t recvBufSize);
    virtual ~AsyncWriterSource();

    /* AsyncEventSource interface */
    virtual int selectFd() const
    { return epollFd_; }
    virtual bool processOne();

    /* enqueue "data" for sending to the service, once the fd becomes
       available for writing */
    bool write(const std::string & data);
    bool write(const char * data, size_t size);
    bool write(std::string && data);

    /* we are ready to accept messages for sending */
    bool canSendMessages() const;

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
    virtual void onDisconnected(bool fromPeer,
                                const std::vector<std::string> & msgs);

    /* invoked when the data is available for reading */
    virtual void onReceivedData(const char * data, size_t size);

    /* invoked when an exception occurs during the handling of events */
    virtual void onException(const std::exception_ptr & excPtr);

    /* number of bytes actually sent */
    uint64_t bytesSent() const
    { return bytesSent_; }

    uint64_t bytesReceived() const
    { return bytesReceived_; }

    /* number of messages actually sent */
    size_t msgsSent() const
    { return msgsSent_; }

    size_t msgsReceived() const
    { return msgsReceived_; }

protected:
    /* writer file descriptor */
    void setFd(int fd);
    int getFd()
        const
    {
        return fd_;
    }
    virtual void closeFd();

    /* epoll operations */
    typedef std::function<void (const ::epoll_event &)> EpollCallback;

    void addFdOneShot(int fd, EpollCallback & cb,
                      bool readerFd, bool writerFd)
    {
        performAddFd(fd, cb, readerFd, writerFd, false);
    }

    void restartFdOneShot(int fd, EpollCallback & cb,
                          bool readerFd, bool writerFd)
    {
        performAddFd(fd, cb, readerFd, writerFd, true);
    }

    void removeFd(int fd);

    /* Return and remove all the messages enqueued for writing. */
    std::vector<std::string> emptyMessageQueue()
    {
        std::vector<std::string> messages
            = threadBuffer_.tryPopMulti(remainingMsgs_);
        remainingMsgs_ -= messages.size();

        return messages;
    }

private:
    void performAddFd(int fd, EpollCallback & cb,
                      bool readerFd, bool writerFd,
                      bool restart);

    /* epoll operations */
    void closeEpollFd();

    /* fd operations */
    void flush();

    void handleFdEvent(const ::epoll_event & event);
    void handleReadReady();
    void handleWriteReady();
    void handleWriteResult(int error,
                           const std::string & written, size_t writtenSize);
    void handleDisconnection(bool fromPeer);
    void handleException();

    /* wakeup operations */
    void handleWakeupEvent(const ::epoll_event & event);

    int epollFd_;
    size_t numFds_;

    int fd_;
    bool closing_;
    size_t recvBufSize_;
    EpollCallback handleFdEventCb_;
    bool writeReady_;

    ML::RingBufferSRMW<std::string> threadBuffer_;
    std::atomic<size_t> remainingMsgs_;
    std::string currentLine_;
    size_t currentSent_;

    uint64_t bytesSent_;
    uint64_t bytesReceived_;
    size_t msgsSent_;
    size_t msgsReceived_;

    ML::Wakeup_Fd wakeup_;
    EpollCallback handleWakeupEventCb_;

    OnDisconnected onDisconnected_;
    OnWriteResult onWriteResult_;
    OnReceivedData onReceivedData_;
    OnException onException_;
};

}
