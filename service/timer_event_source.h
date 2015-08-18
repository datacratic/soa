/* timer_event_source.h                                            -*- C++ -*-
   Wolfgang Sourdeau, August 2015
   Copyright (c) 2015 Datacratic.  All rights reserved.

   A class used internally by MessageLoop to enable multiple timers using the
   same timer fd: thereby reducing the number of file descriptors and context
   switches required to handle such timers.
*/

#pragma once

#include <mutex>
#include "soa/types/date.h"
#include "async_event_source.h"


namespace Datacratic {

/****************************************************************************/
/* TIMER EVENT SOURCE                                                       */
/****************************************************************************/

struct TimerEventSource : public AsyncEventSource {
    /* Type of callback invoked when a timer tick occurs. The callback should
     * return "true" to indicate that the timer must be rescheduled or "false"
     * otherwise. */
    typedef std::function<bool (uint64_t)> OnTick;

    TimerEventSource();
    ~TimerEventSource();

    virtual int selectFd() const;
    virtual bool processOne();

    /* Adds a timer */
    void addTimer(double delay, const OnTick & onTick);

private:
    typedef std::mutex TimersLock;
    typedef std::unique_lock<TimersLock> TimersGuard;

    /* timers */
    struct Timer {
        double interval;
        OnTick onTick;
        Date nextTick;
        Date lastTick;
    };

    void onTimerTick();
    void insertTimer(std::shared_ptr<Timer> && timer);
    std::vector<std::shared_ptr<Timer> > collectTriggeredTimers(Date refDate);
    void adjustNextTick(Date now);

    int timerFd_;

    TimersLock timersLock_;
    std::vector<std::shared_ptr<Timer> > timerQueue_;
    Date nextTick_;
};

} // namespace Datacratic

