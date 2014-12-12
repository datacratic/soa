/* singleton_loop.cc
   Wolfgang Sourdeau, December 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/


#include "async_writer_source.h"
#include "singleton_loop.h"


using namespace std;
using namespace Datacratic;


/****************************************************************************/
/* HTTP CLIENT LOOP ADAPTOR                                                 */
/****************************************************************************/

SingletonLoopAdaptor::
SingletonLoopAdaptor()
    : AsyncWriterSource(nullptr, nullptr, nullptr, 0, 0)
{
}

SingletonLoopAdaptor::
~SingletonLoopAdaptor()
{
}

void
SingletonLoopAdaptor::
addSource(AsyncEventSource & newSource)
{
    int fd = newSource.selectFd();
    auto callback = [&] (const ::epoll_event &) {
        newSource.processOne();
    };
    registerFdCallback(fd, callback);
    addFd(fd, true, false);
}

void
SingletonLoopAdaptor::
removeSource(AsyncEventSource & source)
{
    int fd = source.selectFd();
    removeFd(fd);
    unregisterFdCallback(fd, true);
}


/****************************************************************************/
/* HTTP CLIENT LOOP                                                         */
/****************************************************************************/

SingletonLoop::
SingletonLoop()
    : started_(false),
      adaptor_(new SingletonLoopAdaptor())
{
}

SingletonLoop::
~SingletonLoop()
{
    shutdown();
}

void
SingletonLoop::
start()
{
    if (!started_) {
        loop_.start();
        loop_.addSource("adaptor", adaptor_);
        started_ = true;
    }
}

void
SingletonLoop::
shutdown()
{
    if (started_) {
        loop_.removeSourceSync(adaptor_.get());
        loop_.shutdown();
        started_ = false;
    }
}

void
SingletonLoop::
addSource(AsyncEventSource & newSource)
{
    adaptor_->addSource(newSource);
}

void
SingletonLoop::
removeSource(AsyncEventSource & source)
{
    adaptor_->removeSource(source);
}


