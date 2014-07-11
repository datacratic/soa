/* async_writer_source.cc
   Wolfgang Sourdeau, April 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   A base class for handling writable file descriptors.
*/

#include <fcntl.h>
#include <sys/epoll.h>
#include <poll.h>
#include <unistd.h>

#include "jml/utils/exc_assert.h"
#include "jml/utils/file_functions.h"

#include "async_writer_source.h"

using namespace std;
using namespace Datacratic;


AsyncWriterSource::
AsyncWriterSource(const OnDisconnected & onDisconnected,
                  const OnWriteResult & onWriteResult,
                  const OnReceivedData & onReceivedData,
                  const OnException & onException,
                  size_t maxMessages,
                  size_t recvBufSize)
    : AsyncEventSource(),
      epollFd_(-1),
      numFds_(0),
      fd_(-1),
      closing_(false),
      recvBufSize_(recvBufSize),
      writeReady_(false),
      threadBuffer_(maxMessages),
      remainingMsgs_(0),
      currentSent_(0),
      bytesSent_(0),
      bytesReceived_(0),
      msgsSent_(0),
      msgsReceived_(0),
      wakeup_(EFD_NONBLOCK | EFD_CLOEXEC),
      onDisconnected_(onDisconnected),
      onReceivedData_(onReceivedData),
      onException_(onException)
{
    epollFd_ = ::epoll_create(2);
    if (epollFd_ == -1)
        throw ML::Exception(errno, "epoll_create");

    handleFdEventCb_ = [&] (const ::epoll_event & event) {
        this->handleFdEvent(event);
    };

    handleWakeupEventCb_ = [&] (const ::epoll_event & event) {
        this->handleWakeupEvent(event);
    };
    addFdOneShot(wakeup_.fd(), handleWakeupEventCb_, true, false);
}

AsyncWriterSource::
~AsyncWriterSource()
{
    // cerr << "~AsyncWriterSource\n";
    if (fd_ != -1) {
        // cerr << "closing fd: " + to_string(fd_) + "\n";
        closeFd();
    }
    closeEpollFd();
}

void
AsyncWriterSource::
setFd(int newFd)
{
    if (!ML::is_file_flag_set(newFd, O_NONBLOCK)) {
        throw ML::Exception("file decriptor is blocking");
    }

    ExcCheck(fd_ == -1, "fd already set");
    fd_ = newFd;
    handleFdEventCb_ = [&] (const ::epoll_event & event) {
        this->handleFdEvent(event);
    };
    addFdOneShot(fd_, handleFdEventCb_, true, true);
}

void
AsyncWriterSource::
closeFd()
{
    // cerr << "closeFd...\n";
    ExcCheck(!threadBuffer_.couldPop(),
             "message queue not empty");
    ExcCheck(fd_ != -1, "already closed (fd)");

    try {
        removeFd(fd_);
    }
    catch(const ML::Exception & exc)
    {}
    ::close(fd_);
    // cerr << "fd " + to_string(fd_) + " now closed\n";
    handleDisconnection(false);
    fd_ = -1;
}

void
AsyncWriterSource::
closeEpollFd()
{
    if (epollFd_ == -1) 
        return;

    ::close(epollFd_);
    epollFd_ = -1;
}

bool
AsyncWriterSource::
write(const string & data)
{
    return write(data.c_str(), data.size());
}

bool
AsyncWriterSource::
write(const char * data, size_t size)
{
    return write(string(data, size));
}

bool
AsyncWriterSource::
write(string && data)
{
    bool result(true);

    if (canSendMessages()) {
        if (threadBuffer_.tryPush(move(data))) {
            remainingMsgs_++;
            wakeup_.signal();
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

bool
AsyncWriterSource::
canSendMessages()
    const
{
    return fd_ != -1;
}

void
AsyncWriterSource::
handleReadReady()
{
    char buffer[recvBufSize_];

    cerr << "handleReadReady\n";
    errno = 0;
    while (1) {
        ssize_t s = ::read(fd_, buffer, recvBufSize_);
        // ::fprintf(stderr, "read result: %ld, errno: %d\n", s, errno);
        if (s > 0) {
            msgsReceived_++;
            bytesReceived_ += s;
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
AsyncWriterSource::
handleWriteReady()
{
    writeReady_ = true;
    // cerr << "flush from write ready\n";
    flush();
    // }
}

void
AsyncWriterSource::
handleWriteResult(int error,
                  const string & written, size_t writtenSize)
{
    onWriteResult(error, written, writtenSize);
}

void
AsyncWriterSource::
handleException()
{
    onException(current_exception());
}


void
AsyncWriterSource::
onDisconnected(bool fromPeer, const vector<string> & msgs)
{
    if (onDisconnected_) {
        onDisconnected_(fromPeer, msgs);
    }
}

void
AsyncWriterSource::
onWriteResult(int error,
              const string & written, size_t writtenSize)
{
    if (onWriteResult_) {
        onWriteResult_(error, written, writtenSize);
    }
}

void
AsyncWriterSource::
onReceivedData(const char * buffer, size_t bufferSize)
{
    if (onReceivedData_) {
        onReceivedData_(buffer, bufferSize);
    }
}

void
AsyncWriterSource::
onException(const exception_ptr & excPtr)
{
    if (onException_) {
        onException(excPtr);
    }
}

void
AsyncWriterSource::
requestClose()
{
    if (canSendMessages()) {
        cerr << "requesting close\n";
        closing_ = true;
        wakeup_.signal();
    }
    else {
        cerr << "already disconnected/ing\n";
    }
}

/* async event source */
bool
AsyncWriterSource::
processOne()
{
    struct epoll_event events[numFds_];

    // cerr << "sizeof(events): " + to_string() + "\n";
    try {
        int res = epoll_wait(epollFd_, events, numFds_, 0);
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

/* wakeup events */

void
AsyncWriterSource::
handleWakeupEvent(const ::epoll_event & event)
{
    if ((event.events & EPOLLIN) != 0) {
        eventfd_t val;
        wakeup_.tryRead(val);
        restartFdOneShot(wakeup_.fd(), handleWakeupEventCb_, true, false);

        if (writeReady_) {
            // cerr << "flush from wakeup\n";
            flush();
        }

        if (closing_) {
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
AsyncWriterSource::
flush()
{
    if (!writeReady_) {
        cerr << "BAD: not ready for writing\n";
    }

    auto popLine = [&] {
        bool result;

        /* TODO: atomic test and set */
        if (remainingMsgs_ > 0) {
            if (!threadBuffer_.tryPop(currentLine_)) {
                throw ML::Exception("inconsistency between number of"
                                    " remaining messages and the actual state"
                                    " of the queue");
            }
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
        ssize_t len = ::write(fd_, data, remaining);
        // ::fprintf(stderr, "write result: %ld, remaining: %ld,"
        //           "  errno: %d\n", len, remaining, errno);
        if (len > 0) {
            currentSent_ += len;
            remaining -= len;
            bytesSent_ += len;
            if (remaining == 0) {
                msgsSent_++;
                handleWriteResult(0, currentLine_, currentLine_.size());
                if (popLine()) {
                    data = currentLine_.c_str();
                    remaining = currentLine_.size();
                    ExcAssert(remaining > 0);
                }
                else {
                    currentLine_.clear();
                    done = true;
                }
            }
        }
        else if (len < 0) {
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

/* fd events */

void
AsyncWriterSource::
handleFdEvent(const ::epoll_event & event)
{
    // cerr << "handleFdEvent\n";
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

    if (fd_ != -1) {
        restartFdOneShot(fd_, handleFdEventCb_,
                         true,
                         !writeReady_);
    }
}

void
AsyncWriterSource::
handleDisconnection(bool fromPeer)
{
    // cerr << "handleDisconnection: " + to_string(fd_) + "\n";
    if (fd_ != -1) {
        if (fromPeer) {
            removeFd(fd_);
            ::close(fd_);
        }
        fd_ = -1;
        writeReady_ = false;

        vector<string> lostMessages
            = threadBuffer_.tryPopMulti(remainingMsgs_);
        onDisconnected(fromPeer, lostMessages);
    }
}

/* epoll operations */

void
AsyncWriterSource::
performAddFd(int fd, EpollCallback & cb,
             bool readerFd, bool writerFd,
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
    if (!restart) {
        numFds_++;
    }
}

void
AsyncWriterSource::
removeFd(int fd)
{
    if (epollFd_ == -1)
        return;
    //cerr << Date::now().print(4) << "removed " << fd << endl;

    // ::fprintf(stderr, "epoll_ctl: remove fd=%d\n", fd);
    int res = epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, 0);
    if (res == -1)
        throw ML::Exception(errno, "epoll_ctl DEL " + to_string(fd));
    if (numFds_ == 0) {
        throw ML::Exception("inconsistent number of fds registered");
    }
    numFds_--;
}
