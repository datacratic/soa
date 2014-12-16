/* asio_threaded_loop.cc
   Wolfgang Sourdeau, 15 Dec 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

#include <iostream>
#include "jml/arch/futex.h"

#include "asio_threaded_loop.h"


using namespace std;
using namespace boost::asio;
using namespace Datacratic;


/****************************************************************************/
/* ASIO THREADED LOOP                                                       */
/****************************************************************************/

void
AsioThreadedLoop::
start(const OnStarted & onStarted)
{
    unique_lock<mutex> guard(workLock_);
    if (!work_) {
        work_.reset(new io_service::work(ioService_));
        auto loopFn = [&] () {
            ioService_.run();
        };
        loopThread_ = thread(loopFn);
    }
    if (onStarted) {
        ioService_.post(onStarted);
    }
}

void
AsioThreadedLoop::
startSync()
{
    int started(false);
    auto onStarted = [&] () {
        started = true;
        ML::futex_wake(started);
    };
    start(onStarted);
    while (!started) {
        ML::futex_wait(started, false);
    }
}

void
AsioThreadedLoop::
stop(const OnStopped & onStopped)
{
    unique_lock<mutex> guard(workLock_);
    if (work_) {
        work_.reset();
        ioService_.stop();
        loopThread_.join();
    }
    if (onStopped) {
        onStopped();
    }
}

void
AsioThreadedLoop::
stopSync()
{
    int stopped(false);
    auto onStopped = [&] () {
        stopped = true;
        ML::futex_wake(stopped);
    };
    stop(onStopped);
    while (!stopped) {
        ML::futex_wait(stopped, false);
    }
}
