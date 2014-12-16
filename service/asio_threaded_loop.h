/* asio_threaded_loop.h                                              -*-C++-*-
   Wolfgang Sourdeau, 15 Dec 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <boost/asio/io_service.hpp>


namespace Datacratic {

/****************************************************************************/
/* ASIO THREADED LOOP                                                       */
/****************************************************************************/

/* An Asio io_service running under a secondary thread, similarly to the
 * MessageLoop */
struct AsioThreadedLoop {
    ~AsioThreadedLoop();

    typedef std::function<void()> OnStarted;
    typedef std::function<void()> OnStopped;

    void start(const OnStarted & onStarted = nullptr);
    void startSync();

    void stop(const OnStopped & onStopped = nullptr);
    void stopSync();

    boost::asio::io_service & getIoService()
    {
        return ioService_;
    }

private:
    boost::asio::io_service ioService_;

    std::mutex workLock_;
    std::unique_ptr<boost::asio::io_service::work> work_;
    std::thread loopThread_;
};

} // namespace Datacratic
