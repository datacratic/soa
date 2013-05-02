/* endpoint.h                                                      -*- C++ -*-
   Jeremy Barnes, 21 February 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Generic endpoint; will be subclassed for a particular connection type.
*/

#ifndef __rtb__endpoint_h__
#define __rtb__endpoint_h__

#include <unordered_map>
#include <ace/Synch.h>
#include <ace/Guard_T.h>
#include <set>
#include <boost/function.hpp>
#include <boost/thread/thread.hpp>
#include <iostream>
#include "jml/arch/exception.h"
#include "jml/arch/backtrace.h"
#include "jml/utils/smart_ptr_utils.h"
#include "jml/arch/wakeup_fd.h"
#include "transport.h"
#include "connection_handler.h"
#include "soa/service/epoller.h"
#include <map>


namespace Datacratic {


struct ConnectionHandler;
struct EndpointBase;
struct PassiveEndpoint;

/*****************************************************************************/
/* ENDPOINT BASE                                                             */
/*****************************************************************************/

struct EndpointBase : public Epoller {

    EndpointBase(const std::string & name);

    virtual ~EndpointBase();

    /** Add the current thread to those servicing requests.  It will run
        until the endpoint is finished, either because notifyFinished()
        is called or because the checkFinished callback returns true and
        the kick() method is called.

        Note that it is only necessary to call this method if it was
        initialized with threads == 0.  Otherwise, there are threads already
        running.
    */
    void useThisThread();

    /** Shutdown everything in the bid manager.  Opposite of init().  Will
        implicitly call notifyFinished() and wait for all threads to
        exit before returning.
    */
    void shutdown();

    /** Add a periodic job to be performed by the loop.  The number passed
        to the toRun function is the number of timeouts that have elapsed
        since the last call; this is useful to know if something has
        got behind.  It will normally be 1. */
    typedef std::function<void (uint64_t)> OnTimer;
    void addTimer(double timePeriodSeconds, OnTimer onTimer);

    /** What host are we connected to? */
    virtual std::string hostname() const = 0;

    /** What port are we listening on? */
    virtual int port() const = 0;

    /** Function that will be called to know if we're finished. */
    boost::function<bool ()> onCheckFinished;

    /** Sleep until there are no active connections. */
    void sleepUntilIdle() const;

    int threadsActive() const { return threadsActive_; }

    /** Dump the state of the endpoint for debugging. */
    virtual void dumpState() const;
    
    /** Return the number of connections for this client. */
    virtual int numConnections() const;

    /** Return the number of connections by host. */
    virtual std::map<std::string, int> numConnectionsByHost() const;

    /** Thing to notify when a connection is closed.  Will be called
        before the normal cleanup.
    */
    typedef boost::function<void (TransportBase *)> OnTransportEvent;
    OnTransportEvent onTransportOpen, onTransportClose;

    const std::string & name() const { return name_; }

    /** Set this endpoint up to handle events in realtime. */
    void makeRealTime(int priority = 1);

    /** Spin up the threads as part of the initialization.  NOTE: make sure that this is
        only called once; normally it will be done as part of init().  Calling directly is
        only for advanced use where init() is not called.
    */
    virtual void spinup(int num_threads, bool synchronous);

protected:

    /** Callback to check in the loop if we're finished or not */
    bool checkFinished() const
    {
        if (onCheckFinished) return onCheckFinished();
        return false;
    }

    /** Factory method to associate a connection with a transport. */
    virtual void
    associateHandler(const std::shared_ptr<TransportBase> & transport)
    {
        if (!transport->hasSlave())
            throw ML::Exception("either makeNewTransport or associateHandler"
                                "need to be overridden to make a handler");
    }
    
    virtual void closePeer() = 0;

    struct SPLess {
        template<typename SP>
        bool operator () (const SP & sp1, const SP & sp2) const
        {
            return sp1.get() < sp2.get();
        }
    };

    typedef std::set<std::shared_ptr<TransportBase>, SPLess> Connections;

    /** Set of alive connections.  Used to know what connections are
        outstanding and to keep them alive while they are owned by the
        endpoint system.
    */
    Connections alive;

    /** Tell the endpoint that a connection has been opened. */
    virtual void
    notifyNewTransport(const std::shared_ptr<TransportBase> & transport);

    /** Tell the endpoint that a connection has been closed. */
    virtual void
    notifyCloseTransport(const std::shared_ptr<TransportBase> & transport);

    /** Tell the endpoint that a connection has been recycled.   Default
        simply forwards to notifyCloseTransport.
    */
    virtual void
    notifyRecycleTransport(const std::shared_ptr<TransportBase> & transport);

    /** Re-enable polling after a transport has had it's one-shot event
        handler fire.
    */
    virtual void restartPolling(TransportBase * transport);

    /** Add the transport to the set of events to be polled. */
    virtual void startPolling(TransportBase * transport);

    /** Remove the transport from the set of events to be polled. */
    virtual void stopPolling(TransportBase * transport);

    /** Perform the given callback asynchronously (in a worker thread) in the
        context of the given transport.
    */
    void doAsync(const std::shared_ptr<TransportBase> & transport,
                 const boost::function<void ()> & callback,
                 const char * nameOfCallback);

    typedef ACE_Recursive_Thread_Mutex Lock;
    typedef ACE_Guard<Lock> Guard;
    
    mutable Lock lock;

    /** released when there are no active connections */
    mutable ACE_Semaphore idle;
    
    /** Should the endpoint class manipulate the idle count? */
    mutable bool modifyIdle;

private:
    std::string name_;
    std::unique_ptr<boost::thread_group> eventThreads;
    std::vector<boost::thread *> eventThreadList;
    int threadsActive_;

    friend class TransportBase;
    friend class ConnectionHandler;
    template<typename Transport> friend class ConnectorT;

    /* Number of active FDs in items */
    int numTransports;

    /* FD we can use to wake up the event loop */
    ML::Wakeup_Fd wakeup;

    /* Are we shutting down? */
    bool shutdown_;

    std::map<std::string, int> numTransportsByHost;

    /** Run a thread to handle events. */
    void runEventThread(int threadNum, int numThreads);

    struct EpollData {
        enum EpollDataType {
            INVALID,
            FD,
            TIMER
        };

        EpollData(EpollData::EpollDataType fdType, int fd)
            : fdType(fdType), fd(fd)
        {
        }

        EpollDataType fdType;
        int fd;

        TransportBase *transport; /* FD */
        OnTimer onTimer;          /* TIMER */
    };
    
    typedef std::unordered_map<int, EpollData> EpollDataByFd;
    EpollDataByFd epollDataByFd;

    /** Handle a single ePoll event */
    bool handleEpollEvent(epoll_event & event);
    void handleFdEvent(const EpollData & eventData);
    void handleTimerEvent(const EpollData & eventData);
};

} // namespace Datacratic

#endif /* __rtb__endpoint_h__ */
