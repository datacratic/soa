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
    : timerFd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)),
      counter_(1),
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
    vector<Timer> triggered = collectTriggeredTimers(now);

    for (auto & timer: triggered) {
        double interval = timer.lastTick.secondsUntil(now);
        uint64_t numTicks = (uint64_t) ::floor(interval / timer.interval);
        if (timer.onTick(numTicks)) {
            timer.lastTick = now;
            timer.nextTick = now.plusSeconds(timer.interval);
            insertTimer(move(timer));
        }
    }

    adjustNextTick(now);
}

vector<TimerEventSource::Timer>
TimerEventSource::
collectTriggeredTimers(Date refDate)
{
    vector<Timer> triggered;
    TimersGuard guard(timersLock_);

    size_t nbrTriggered, nbrTimers(timerQueue_.size());
    for (nbrTriggered = 0; nbrTriggered < nbrTimers; nbrTriggered++) {
        if (timerQueue_[nbrTriggered].nextTick > refDate) {
            break;
        }
    }

    if (nbrTriggered > 0) {
        triggered.reserve(nbrTriggered);
        for (size_t i = 0; i < nbrTriggered; i++) {
            triggered.emplace_back(move(timerQueue_[i]));
        }
        timerQueue_.erase(timerQueue_.begin(), timerQueue_.begin() + nbrTriggered);
    }

    return triggered;
}

void
TimerEventSource::
adjustNextTick(Date now)
{
    TimersGuard guard(timersLock_);

    Date negInfinity = Date::negativeInfinity();
    Date nextTick = ((timerQueue_.size() > 0)
                     ? timerQueue_[0].nextTick
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

uint64_t
TimerEventSource::
addTimer(double delay, const OnTick & onTick)
{
    Date now = Date::now();
    uint64_t timerId = counter_.fetch_add(1);
    ExcAssert(timerId != 0); // ensure we never reach the upper limit during a
                             // program's lifetime

    Timer newTimer{delay, onTick};
    newTimer.nextTick = now.plusSeconds(delay);
    newTimer.lastTick = Date::negativeInfinity();
    newTimer.timerId = timerId;
    insertTimer(move(newTimer));
    adjustNextTick(now);

    return timerId;
}

void
TimerEventSource::
insertTimer(Timer && timer)
{
    TimersGuard guard(timersLock_);

    auto timerCompare = [&] (const Timer & left,
                             const Timer & right) {
        return left.nextTick < right.nextTick;
    };
    auto loc = lower_bound(timerQueue_.begin(), timerQueue_.end(),
                           timer, timerCompare);
    timerQueue_.insert(loc, move(timer));
}

bool
TimerEventSource::
cancelTimer(uint64_t timerId)
{
    TimersGuard guard(timersLock_);

    for (auto it = timerQueue_.begin(); it != timerQueue_.end(); it++) {
        if (it->timerId == timerId) {
            timerQueue_.erase(it);
            return true;
        }
    }

    return false;
}
