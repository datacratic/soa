#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <atomic>
#include <iostream>

#include <boost/test/unit_test.hpp>

#include "jml/arch/timers.h"
#include "jml/utils/testing/watchdog.h"
#include "soa/types/date.h"
#include "soa/service/message_loop.h"
#include "soa/service/timer_event_source.h"

using namespace std;
using namespace Datacratic;


BOOST_AUTO_TEST_CASE( test_addTimer )
{
    ML::Watchdog wd(10);
    std::atomic<int> ticks(0);
    MessageLoop loop(1, 0, -1);
    loop.start();
    auto timer = make_shared<TimerEventSource>();
    loop.addSource("timer", timer);
    timer->waitConnectionState(AsyncEventSource::CONNECTED);

    auto onTick = [&] (uint64_t) {
        Date now = Date::now();
        ticks++;
        return ticks < 3;
    };
    timer->addTimer(0.2, onTick);

    while (true) {
        if (ticks == 3) {
            Date now = Date::now();
            break;
        }
        ML::sleep(1);
    }

    loop.shutdown();
}


BOOST_AUTO_TEST_CASE( test_cancelTimer )
{
    ML::Watchdog wd(10);
    std::atomic<int> ticks(0);
    MessageLoop loop(1, 0, -1);
    loop.start();
    auto timer = make_shared<TimerEventSource>();
    loop.addSource("timer", timer);
    timer->waitConnectionState(AsyncEventSource::CONNECTED);

    auto onTick = [&] (uint64_t) {
        Date now = Date::now();
        ticks++;
        return true;
    };
    auto timerId = timer->addTimer(0.2, onTick);
    BOOST_CHECK(timerId > 0);

    ML::sleep(1);
    BOOST_CHECK_EQUAL(timer->cancelTimer(timerId), true);
    BOOST_CHECK_NE(ticks, 0);
    int oldTicks = ticks;
    ML::sleep(1);
    BOOST_CHECK_EQUAL(ticks, oldTicks);
    BOOST_CHECK_EQUAL(timer->cancelTimer(timerId), false);

    loop.shutdown();
}
