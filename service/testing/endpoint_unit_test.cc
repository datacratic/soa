/* endpoint_test.cc
   Jeremy Barnes, 31 January 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   Tests for the endpoints.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <sys/eventfd.h>

#include <boost/test/unit_test.hpp>
#include "soa/service/endpoint.h"
#include "soa/service/http_endpoint.h"
#include "soa/service/active_endpoint.h"
#include "soa/service/passive_endpoint.h"
#include <sys/socket.h>
#include "jml/utils/guard.h"
#include "jml/arch/exception_handler.h"
#include "jml/utils/testing/watchdog.h"
#include "jml/utils/testing/fd_exhauster.h"
#include "test_connection_error.h"

using namespace std;
using namespace ML;
using namespace Datacratic;

#if 0 // zeromq aborts when it can't get an fd
BOOST_AUTO_TEST_CASE( test_passive_endpoint_create_no_fds )
{
    Watchdog watchdog;
    FDExhauster exhaust_fds;

    PassiveEndpointT<SocketTransport> endpoint;

    {
        JML_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(endpoint.init(), ML::Exception);
    }

    endpoint.shutdown();
}
#endif

#if 0  // we need more than one FD for each connection now; test unreliable
BOOST_AUTO_TEST_CASE( test_active_endpoint_create_no_fds )
{
    BOOST_REQUIRE_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_REQUIRE_EQUAL(ConnectionHandler::created,
                        ConnectionHandler::destroyed);

    Watchdog watchdog;
    
    ActiveEndpointT<SocketTransport> connector("connector");
    connector.init(9997, "localhost", 0, 1, true, false /* throw on error */);

    {
        FDExhauster exhaust_fds;
        
        cerr << "doing connection error" << endl;

        doTestConnectionError(connector, "Too many open files");

        cerr << "done connection error" << endl;
    }

    connector.shutdown();

    BOOST_CHECK_EQUAL(connector.numActiveConnections(), 0);
    BOOST_CHECK_EQUAL(connector.numInactiveConnections(), 0);
    BOOST_CHECK_EQUAL(connector.threadsActive(), 0);
    BOOST_CHECK_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_CHECK_EQUAL(ConnectionHandler::created,
                      ConnectionHandler::destroyed);
}
#endif

#if 0 // no way yet to make work be done until sem acquired
BOOST_AUTO_TEST_CASE( test_active_endpoint_no_threads )
{
    Watchdog watchdog;
    
    ActiveEndpointT<SocketTransport> connector;
    connector.init(9997, "localhost", 0, 0, true);

    doTestConnectionError(connector, "not connected");
}
#endif

BOOST_AUTO_TEST_CASE( test_passive_endpoint )
{
    BOOST_REQUIRE_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_REQUIRE_EQUAL(ConnectionHandler::created,
                        ConnectionHandler::destroyed);

    Watchdog watchdog(5.0);

    string connectionError;

    PassiveEndpointT<SocketTransport> acceptor("acceptor");
    int port = acceptor.init();

    BOOST_CHECK_NE(port, -1);

    acceptor.shutdown();

    BOOST_CHECK_EQUAL(TransportBase::created, TransportBase::destroyed);
    BOOST_CHECK_EQUAL(ConnectionHandler::created,
                      ConnectionHandler::destroyed);
}

#if 0
namespace {

struct MockEndpoint : public Datacratic::EndpointBase {
    MockEndpoint (const std::string & name)
        : EndpointBase(name)
    {
    }

    virtual std::string hostname()
        const
    {
        return "mock-ep";
    }
    
    virtual int port()
        const
    {
        return -1;
    }

    virtual void closePeer()
    {}
};

}

BOOST_AUTO_TEST_CASE( test_EndpointBase_handleTimerEvent )
{
    /* FIXME: this test currenly ensure that the thread assignment mechanism
       works */
    Watchdog watchdog(5.0);

    int timerFd = eventfd(0, 0);
    uint64_t eventData(12345);
    int x = ::write(timerFd, &eventData, sizeof(eventData));
    BOOST_CHECK_EQUAL(x, sizeof(eventData));

    EndpointBase::EpollData timerData(EndpointBase::EpollData::TIMER, timerFd);

    int timerInvoked(0);
    uint64_t lastNums(0);
    timerData.onTimer = [&] (uint64_t nums) {
        timerInvoked++;
        lastNums = nums;
    };

    MockEndpoint anEndpoint("myep");
    bool rc = anEndpoint.handleTimerEvent(timerData);

    /* must have invoked timer */
    BOOST_CHECK_EQUAL(timerInvoked, 1);
    BOOST_CHECK_EQUAL(lastNums, 12345);
    BOOST_CHECK_EQUAL(rc, false);
    BOOST_CHECK_NE(timerData.threadId, 0); /* thread id must be assigned */

    pid_t threadId = timerData.threadId;
    timerData.threadId = threadId^0xffffffff;
    eventData = 54321;
    x = ::write(timerFd, &eventData, sizeof(eventData));
    BOOST_CHECK_EQUAL(x, sizeof(eventData));
    rc = anEndpoint.handleTimerEvent(timerData);
    /* must not have invoked timer */
    BOOST_CHECK_EQUAL(timerInvoked, 1);
    BOOST_CHECK_EQUAL(lastNums, 12345);
    BOOST_CHECK_EQUAL(rc, true);
    BOOST_CHECK_NE(timerData.threadId, threadId);
}
#endif
