/** loop_monitor.cc                                 -*- C++ -*-
    Rémi Attab, 06 May 2013
    Copyright (c) 2013 Datacratic.  All rights reserved.

    Implementation details of the message loop monitor.

*/

#include "loop_monitor.h"

#include <mutex>
#include <functional>

using namespace std;
using namespace ML;

namespace Datacratic {


/******************************************************************************/
/* LOOP MONITOR                                                               */
/******************************************************************************/

LoopMonitor::
LoopMonitor(const shared_ptr<ServiceProxies> services, const string& name) :
    ServiceBase(name, services)
{}

LoopMonitor::
LoopMonitor(ServiceBase& parent, const string& name) :
    ServiceBase(name, parent)
{}

void
LoopMonitor::
init(double updatePeriod)
{
    MessageLoop::init();

    this->updatePeriod = updatePeriod;
    addPeriodic("LoopMonitor", updatePeriod,
            std::bind(&LoopMonitor::doLoops, this, placeholders::_1));
}

void
LoopMonitor::
doLoops(uint64_t numTimeouts)
{
    std::lock_guard<ML::Spinlock> guard(lock);

    LoadSample maxLoad;
    maxLoad.sequence = curLoad.sequence + 1;

    for (auto& loop : loops) {
        double load = loop.second();
        ExcAssertGreaterEqual(load, 0.0);
        ExcAssertLessEqual(load, 1.0);

        if (load > maxLoad.load) maxLoad.load = load;
        recordLevel(load, loop.first);
    }

    curLoad.packed = maxLoad.packed;
}

void
LoopMonitor::
addMessageLoop(const string& name, const MessageLoop* loop)
{
    double lastTimeSlept = 0.0; // acts as a private member variable for sampleFn

    auto sampleFn = [=] () mutable {
        double timeSlept = loop->totalSleepSeconds();
        double delta = timeSlept - lastTimeSlept;
        lastTimeSlept = timeSlept;

        return 1.0 - (delta / updatePeriod);
    };

    addCallback(name, sampleFn);
}

void
LoopMonitor::
addCallback(const string& name, const SampleLoadFn& cb)
{
    std::lock_guard<ML::Spinlock> guard(lock);

    auto ret = loops.insert(make_pair(name, cb));
    ExcCheck(ret.second, "loop already being monitored: " + name);
}

void
LoopMonitor::
remove(const string& name)
{
    std::lock_guard<ML::Spinlock> guard(lock);

    size_t ret = loops.erase(name);
    ExcCheckEqual(ret, 1, "loop is not monitored: " + name);
}


/******************************************************************************/
/* SIMPLE LOAD SHEDDING                                                       */
/******************************************************************************/

SimpleLoadShedding::
SimpleLoadShedding(const LoopMonitor& loopMonitor) :
    loopMonitor(loopMonitor),
    loadThreshold(0.9),
    shedProb(0.0)
{}

void
SimpleLoadShedding::
updateProb(LoopMonitor::LoadSample sample)
{
    lastSample = sample;

    // Don't drop too low otherwise it'll take forever to raise the prob.
    if (sample.load < loadThreshold)
        shedProb = std::max(0.01, shedProb - 0.01);

    // Should drop faster then it raises so that we're responsive to load spikes
    else shedProb = std::min(1.0, shedProb + 0.05);
}


} // namepsace Datacratic
