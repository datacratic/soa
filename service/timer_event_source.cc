/* timer_event_source.cc
   Wolfgang Sourdeau, August 2015
   Copyright (c) 2015 Datacratic.  All rights reserved.
*/

#include <math.h>
#include <sys/timerfd.h>

#include "jml/utils/exc_assert.h"
#include "timer_event_source.h"

using namespace std;
using namespace Datacratic;


/****************************************************************************/
/* TIMER EVENT SOURCE                                                       */
/****************************************************************************/

TimerEventSource::
TimerEventSource()
    : timerFd_(::timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK | TFD_CLOEXEC)),
      nextTick_(Date::negativeInfinity())
{
    if (timerFd_ == -1) {
        throw ML::Exception(errno, "timerfd_create");
    }
}

TimerEventSource::
~TimerEventSource()
{
    int res = ::close(timerFd_);
    if (res == -1) {
        cerr << "warning: close on timerfd: " << strerror(errno) << endl;
    }
}

int
TimerEventSource::
selectFd()
    const
{
    return timerFd_;
}

bool
TimerEventSource::
processOne()
{
start:
    uint64_t numWakeups = 0;
    int res = ::read(timerFd_, &numWakeups, 8);
    if (res == -1) {
        if (errno == EINTR) {
            goto start;
        }
        else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return false;
        }
        else {
            throw ML::Exception(errno, "timerfd read");
        }
    }
    ExcAssertEqual(res, 8);
    onTimerTick();

    return false;
}

void
TimerEventSource::
onTimerTick()
{
    Date now = Date::now();
    vector<shared_ptr<Timer> > triggered = collectTriggeredTimers(now);

    for (auto & timer: triggered) {
        double interval = timer->lastTick.secondsUntil(now);
        uint64_t numTicks = (uint64_t) ::floor(interval / timer->interval);
        if (timer->onTick(numTicks)) {
            timer->lastTick = now;
            timer->nextTick = now.plusSeconds(timer->interval);
            insertTimer(move(timer));
        }
    }

    adjustNextTick(now);
}

vector<shared_ptr<TimerEventSource::Timer> >
TimerEventSource::
collectTriggeredTimers(Date refDate)
{
    vector<shared_ptr<Timer> > triggered;
    vector<shared_ptr<Timer> > newQueue;

    TimersGuard guard(timersLock_);
    for (auto & timer: timerQueue_) {
        auto & target = (timer->nextTick < refDate) ? triggered : newQueue;
        target.emplace_back(move(timer));
    }
    timerQueue_ = move(newQueue);

    return triggered;
}

void
TimerEventSource::
adjustNextTick(Date now)
{
    TimersGuard guard(timersLock_);

    Date negInfinity = Date::negativeInfinity();
    Date nextTick = ((timerQueue_.size() > 0)
                     ? timerQueue_[0]->nextTick
                     : negInfinity);

    if (nextTick != nextTick_) {
        struct itimerspec spec{{0, 0}, {0, 0}};

        if (nextTick != negInfinity) {
            double delta = nextTick - now;
            auto & value = spec.it_value;
            value.tv_sec = (time_t) delta;
            value.tv_nsec = (delta - spec.it_value.tv_sec) * 1000000000;
        }
        int res = ::timerfd_settime(timerFd_, 0, &spec, nullptr);
        if (res == -1) {
            throw ML::Exception(errno, "timerfd_settime");
        }
        nextTick_ = nextTick;
    }
}


void
TimerEventSource::
addTimer(double delay, const OnTick & onTick)
{
    Date now = Date::now();
    auto newTimer = make_shared<Timer>(Timer{delay, onTick});
    newTimer->nextTick = now.plusSeconds(delay);
    newTimer->lastTick = Date::negativeInfinity();
    insertTimer(move(newTimer));
    adjustNextTick(now);
}

void
TimerEventSource::
insertTimer(shared_ptr<Timer> && timer)
{
    TimersGuard guard(timersLock_);

    auto timerCompare = [&] (const shared_ptr<Timer> & left,
                             const shared_ptr<Timer> & right) {
        return left->nextTick < right->nextTick;
    };
    auto loc = lower_bound(timerQueue_.begin(), timerQueue_.end(),
                           timer, timerCompare);
    timerQueue_.insert(loc, move(timer));
}
