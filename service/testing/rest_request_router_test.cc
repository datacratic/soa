/** rest_request_router_test.cc
    Jeremy Barnes, 31 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "soa/service/rest_request_router.h"
#include "soa/service/in_process_rest_connection.h"


using namespace std;
using namespace ML;
using namespace Datacratic;


BOOST_AUTO_TEST_CASE( test_header_matching )
{
    RestRequestRouter router;

    string callDone;

    auto asyncCallback = [&] (RestConnection & connection,
                              const RestRequest & request,
                              RestRequestParsingContext & context)
        {
            connection.sendResponse(200, "async", "text/plain");
            return RestRequestRouter::MR_YES;
        };

    auto syncCallback = [&] (RestConnection & connection,
                              const RestRequest & request,
                              RestRequestParsingContext & context)
        {
            connection.sendResponse(200, "sync", "text/plain");
            return RestRequestRouter::MR_YES;
        };

    router.addRoute("/test", { "GET", "header:async=true" },
                    "Async route", asyncCallback,
                    Json::Value());

    router.addRoute("/test", { "GET" },
                    "Sync route", syncCallback,
                    Json::Value());
    
    RestRequest request;
    request.verb = "GET";
    request.resource = "/test";
    request.header.headers["async"] = "true";

    InProcessRestConnection conn;

    router.handleRequest(conn, request);

    BOOST_CHECK_EQUAL(conn.response, "async");

    request.header.headers.erase("async");

    cerr << "request " << request << endl;

    InProcessRestConnection conn2;

    router.handleRequest(conn2, request);

    BOOST_CHECK_EQUAL(conn2.response, "sync");
}
