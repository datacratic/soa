#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <atomic>
#include <memory>
#include <string>
#include <tuple>
#include <boost/test/unit_test.hpp>

#include "jml/arch/futex.h"
#include "jml/arch/timers.h"
#include "jml/utils/testing/watchdog.h"
#include "soa/service/http_client_v3.h"
#include "soa/utils/print_utils.h"

#include "test_http_services.h"


using namespace std;
using namespace Datacratic;


/* helpers functions used in tests */
namespace {

typedef tuple<HttpClientError, int, string> ClientResponse;

#define CALL_MEMBER_FN(object, pointer)  (object.*(pointer))

/* sync request helpers */
template<typename Func>
ClientResponse
doRequest(const string & baseUrl, const string & resource,
          Func func,
          const RestParams & queryParams, const RestParams & headers,
          int timeout = -1)
{
    ClientResponse response;

    boost::asio::io_service ioService;
    HttpClientV3 client(ioService, baseUrl);

    int done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        ML::futex_wake(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);

    CALL_MEMBER_FN(client, func)(resource, cbs, queryParams, headers,
                                 timeout);
    ioService.run();

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    return response;
}

ClientResponse
doGetRequest(const string & baseUrl, const string & resource,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
{
    return doRequest(baseUrl, resource, &HttpClientV3::get,
                     queryParams, headers, timeout);
}

ClientResponse
doDeleteRequest(const string & baseUrl, const string & resource,
                const RestParams & queryParams = RestParams(),
                const RestParams & headers = RestParams(),
                int timeout = -1)
{
    return doRequest(baseUrl, resource, &HttpClientV3::del,
                     queryParams, headers, timeout);
}

ClientResponse
doUploadRequest(bool isPut,
                const string & baseUrl, const string & resource,
                const string & body, const string & type)
{
    ClientResponse response;

    boost::asio::io_service ioService;
    HttpClientV3 client(ioService, baseUrl);

    int done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           HttpClientError error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        HttpClientError & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        ML::futex_wake(done);
    };

    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    HttpRequest::Content content(body, type);
    if (isPut) {
        client.put(resource, cbs, content);
    }
    else {
        client.post(resource, cbs, content);
    }
    ioService.run();

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    return response;
}

}

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_get )
{
    cerr << "client_get\n";
    ML::Watchdog watchdog(10);
    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    service.addResponse("GET", "/coucou", 200, "coucou");
    service.start();

    service.waitListening();

#if 0
    /* request to bad ip
       Note: if the ip resolution timeout is very high on the router, the
       Watchdog timeout might trigger first */
    {
        ::fprintf(stderr, "request to bad ip\n");
        string baseUrl("http://123.234.12.23");
        auto resp = doGetRequest(baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::CouldNotConnect);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

#if 0
    /* request to bad hostname
       Note: will fail when the name service returns a "default" value for all
       non resolved hosts */
    {
        ::fprintf(stderr, "request to bad hostname\n");
        string baseUrl("http://somewhere.lost");
        auto resp = doGetRequest(baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::HostNotFound);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

#if 0
    /* request with timeout */
    {
        ::fprintf(stderr, "request with timeout\n");
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/timeout", {}, {}, 1);
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::Timeout);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* request connection close  */
    {
        ::fprintf(stderr, "testing behaviour with connection: close\n");
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/connection-close");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 204);
    }

    /* request to /nothing -> 404 */
    {
        ::fprintf(stderr, "request with 404\n");
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 404);
    }

    /* request to /coucou -> 200 + "coucou" */
    {
        ::fprintf(stderr, "request with 200\n");
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/coucou");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 200);
        BOOST_CHECK_EQUAL(get<2>(resp), "coucou");
    }

    /* headers and cookies */
    {
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/headers", {},
                                 {{"someheader", "somevalue"}});
        Json::Value expBody;
        expBody["accept"] = "*/*";
        expBody["host"] = baseUrl.substr(7);
        expBody["someheader"] = "somevalue";
        Json::Value jsonBody = Json::parse(get<2>(resp));
        BOOST_CHECK_EQUAL(jsonBody, expBody);
    }

    /* query-params */
    {
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/query-params",
                                 {{"value", "hello"}});
        string body = get<2>(resp);
        BOOST_CHECK_EQUAL(body, "?value=hello");
    }
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_post )
{
    cerr << "client_post\n";
    ML::Watchdog watchdog(10);
    auto proxies = make_shared<ServiceProxies>();
    HttpUploadService service(proxies);
    service.start();

    /* request to /coucou -> 200 + "coucou" */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doUploadRequest(false, baseUrl, "/post-test",
                                    "post body", "application/x-nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 200);
        Json::Value jsonBody = Json::parse(get<2>(resp));
        BOOST_CHECK_EQUAL(jsonBody["verb"], "POST");
        BOOST_CHECK_EQUAL(jsonBody["payload"], "post body");
        BOOST_CHECK_EQUAL(jsonBody["type"], "application/x-nothing");
    }
}
#endif

#if 0
#if 1
BOOST_AUTO_TEST_CASE( test_http_client_put )
{
    cerr << "client_put\n";
    ML::Watchdog watchdog(10);
    auto proxies = make_shared<ServiceProxies>();
    HttpUploadService service(proxies);
    service.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));
    string bigBody;
    for (int i = 0; i < 65535; i++) {
        bigBody += "this is one big body,";
    }
    auto resp = doUploadRequest(true, baseUrl, "/put-test",
                                bigBody, "application/x-nothing");
    BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
    BOOST_CHECK_EQUAL(get<1>(resp), 200);
    Json::Value jsonBody = Json::parse(get<2>(resp));
    BOOST_CHECK_EQUAL(jsonBody["verb"], "PUT");
    BOOST_CHECK_EQUAL(jsonBody["payload"], bigBody);
    BOOST_CHECK_EQUAL(jsonBody["type"], "application/x-nothing");
}
#endif
#endif

#if 1
BOOST_AUTO_TEST_CASE( http_test_client_delete )
{
    cerr << "client_delete" << endl;
    ML::Watchdog watchdog(10);

    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    service.addResponse("DELETE", "/deleteMe", 200, "Deleted");
    service.start();

    string baseUrl("http://127.0.0.1:" + to_string(service.port()));
    auto resp = doDeleteRequest(baseUrl, "/deleteMe", {}, {}, 1);

    BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
    BOOST_CHECK_EQUAL(get<1>(resp), 200);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_put_multi )
{
    cerr << "client_put_multi\n";
    auto proxies = make_shared<ServiceProxies>();
    HttpUploadService service(proxies);
    service.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    boost::asio::io_service ioService;
    HttpClientV3 client(ioService, baseUrl);

    constexpr size_t maxRequests(500);
    atomic<int> done(0);

    auto loopFn = [&] {
        while (done < maxRequests) {
            cerr << "looping.. " + to_string(done) + "\n";
            ioService.run();
            ML::sleep(0.5);
        }
    };
    thread loopTh(loopFn);

    auto makeBody = [&] (size_t i) {
        int multiplier = (i < maxRequests / 2) ? -2 : 2;
        size_t bodySize = 2000 + multiplier * i;
        string body = ML::format("%.4x", bodySize);
        size_t rndSize = bodySize - body.size();
        body += randomString(rndSize);

        return body;
    };

    for (size_t i = 0; i < maxRequests; i++) {
        if (i == 0) {
            cerr << " started sending requests\n";
        }
        auto sendBody = makeBody(i);
        auto onResponse = [&, sendBody] (const HttpRequest & rq,
                                         HttpClientError error,
                                         int status,
                                         string && headers,
                                         string && body) {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            BOOST_CHECK_EQUAL(status, 200);
            Json::Value jsonBody = Json::parse(body);
            BOOST_CHECK_EQUAL(jsonBody["verb"], "PUT");
            BOOST_CHECK_EQUAL(jsonBody["payload"], sendBody);
            BOOST_CHECK_EQUAL(jsonBody["type"], "text/plain");
            done++;
            if (done == maxRequests) {
                ML::futex_wake(done);
            }
        };

        auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
        HttpRequest::Content content(sendBody, "text/plain");
        while (!client.put("/", cbs, content)) {
            cerr << "put failed\n";
            ML::sleep(0.2);
        }
    };
    
    while (done < maxRequests) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }
    loopTh.join();
}
#endif

#if 1
/* Ensures that all requests are correctly performed under load, including
   when "Connection: close" is encountered once in a while.
   Not a performance test. */
BOOST_AUTO_TEST_CASE( test_http_client_stress_test )
{
    cerr << "stress_test\n";
    // const int mask = 0x3ff; /* mask to use for displaying counts */
    // ML::Watchdog watchdog(300);
    auto proxies = make_shared<ServiceProxies>();
    auto doStressTest = [&] (int numParallel) {
        ::fprintf(stderr, "stress test with %d parallel connections\n",
                  numParallel);

        HttpGetService service(proxies);
        service.start();
        service.waitListening();

        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));

        boost::asio::io_service ioService;
        HttpClientV3 client(ioService, baseUrl, numParallel);

        int maxReqs(30000), missedReqs(0);
        atomic<int> numReqs(0);
        int numResponses(0);

        auto loopFn = [&] {
            while (numReqs < maxReqs) {
                ioService.run();
                ML::sleep(0.5);
            }
        };
        thread loopTh(loopFn);

        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            numResponses++;

            BOOST_CHECK_EQUAL(errorCode, HttpClientError::None);
            BOOST_CHECK_EQUAL(status, 200);

            int bodyNbr;
            try {
                bodyNbr = stoi(body);
            }
            catch (...) {
                ::fprintf(stderr, "exception when parsing body: %s\n",
                          body.c_str());
                throw;
            }

            int lowerLimit = std::max(0, (numResponses - numParallel));
            int upperLimit = std::min(maxReqs, (numResponses + numParallel));
            if (bodyNbr < lowerLimit || bodyNbr > upperLimit) {
                throw ML::Exception("number of returned server requests "
                                    " is anomalous: %d is out of range"
                                    " [%d,*%d,%d]",
                                    bodyNbr, lowerLimit,
                                    numResponses, upperLimit);
            }

            if (numResponses == numReqs) {
                ML::futex_wake(numResponses);
            }
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);

        while (numReqs < maxReqs) {
            const char * url = "/counter";
            if (client.get(url, cbs)) {
                numReqs++;
                // if ((numReqs & mask) == 0 || numReqs == maxReqs) {
                //     ::fprintf(stderr, "performed %d requests\n", numReqs);
                // }
            }
            else {
                missedReqs++;
            }
        }

        ::fprintf(stderr, "all requests performed, awaiting responses...\n");
        while (numResponses < maxReqs) {
            int old(numResponses);
            ML::futex_wait(numResponses, old);
        }
        loopTh.join();
        ::fprintf(stderr, "performed %d requests; missed: %d\n",
                  maxReqs, missedReqs);
    };

    doStressTest(1);
    doStressTest(8);
    doStressTest(128);
}
#endif

#if 0
/* Ensure that the move constructor and assignment operator behave
   reasonably well. */
BOOST_AUTO_TEST_CASE( test_http_client_move_constructor )
{
    cerr << "move_constructor\n";
    ML::Watchdog watchdog(30);
    auto proxies = make_shared<ServiceProxies>();

    HttpGetService service(proxies);
    service.addResponse("GET", "/", 200, "coucou");
    service.start();
    service.waitListening();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    auto doGet = [&] (HttpClientV3 & getClient) {
        int done(false);

        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done = true;
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);

        getClient.get("/", cbs);
        while (!done) {
            int old = done;
            ML::futex_wait(done, old);
        }
    };

    boost::asio::io_service ioService;

    /* move constructor */
    cerr << "testing move constructor\n";
    auto makeClient = [&] () {
        return HttpClientV3(ioService, baseUrl, 1);
    };
    HttpClientV3 client1(move(makeClient()));
    doGet(client1);

    /* move assignment operator */
    cerr << "testing move assignment op.\n";
    HttpClientV3 client2(ioService, "http://nowhere", 1);
    client2 = move(client1);
    doGet(client2);
}
#endif

#if 1
/* Ensure that an infinite number of requests can be queued when queue size is
 * 0, even from within callbacks. */
BOOST_AUTO_TEST_CASE( test_http_client_unlimited_queue )
{
    static const int maxLevel(4);

    ML::Watchdog watchdog(30);
    auto proxies = make_shared<ServiceProxies>();

    HttpGetService service(proxies);
    service.addResponse("GET", "/", 200, "coucou");
    service.start();
    service.waitListening();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    boost::asio::io_service ioService;
    HttpClientV3 client(ioService, baseUrl, 4, 0);

    atomic<int> pending(0);
    int done(0);

    auto loopFn = [&] {
        while (pending > 0) {
            ioService.run();
            ML::sleep(0.5);
        }
    };
    thread loopTh(loopFn);

    function<void(int)> doGet = [&] (int level) {
        pending++;
        auto onDone = [&,level] (const HttpRequest & rq,
                                 HttpClientError errorCode, int status,
                                 string && headers, string && body) {
            if (level < maxLevel) {
                for (int i = 0; i < 10; i++) {
                    doGet(level+1);
                }
            }
            pending--;
            done++;
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/", cbs);
    };

    doGet(0);

    while (pending > 0) {
        ML::sleep(1);
        cerr << "requests done: " + to_string(done) + "\n";
    }
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_expect_100_continue )
{
    ML::Watchdog watchdog(10);
    cerr << "client_expect_100_continue" << endl;

    auto proxies = make_shared<ServiceProxies>();

    HttpUploadService service(proxies);
    service.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    boost::asio::io_service ioService;
    HttpClientV3 client(ioService, baseUrl);
    // client.debug(true);
    // client.sendExpect100Continue(true);

    HttpHeader sentHeaders;

    auto getClientHeader = [&] (const string & body,
                                const string & headerKey) {
        Json::Value jsonValue = Json::parse(body);
        return jsonValue["headers"][headerKey].asString();
    };

    {
        int done(false);
        string body;
        auto callbacks = std::make_shared<HttpClientSimpleCallbacks>(
            [&] (const HttpRequest &, HttpClientError error,
                 int statusCode, std::string &&,
                 std::string && newBody) {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            BOOST_CHECK_EQUAL(statusCode, 200);
            body = move(newBody);
            done = true;
            ML::futex_wake(done);
        });

        const std::string& smallPayload = randomString(20);
        HttpRequest::Content content(smallPayload, "application/x-nothing");
        client.post("/post-test", callbacks, content);

        while (!done) {
            ML::futex_wait(done, false);
        }

        BOOST_CHECK_EQUAL(getClientHeader(body, "expect"), "");
    }

    {
        int done(false);
        string body;
        auto callbacks = std::make_shared<HttpClientSimpleCallbacks>(
            [&](const HttpRequest&, HttpClientError error,
                int statusCode, std::string &&,
                std::string&& newBody) {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            body = move(newBody);
            done = true;
            ML::futex_wake(done);
        });

        const std::string& bigPayload = randomString(2024);
        HttpRequest::Content content(bigPayload, "application/x-nothing");
        client.post("/post-test", callbacks, content);

        while (!done) {
            ML::futex_wait(done, false);
        }

        // BOOST_CHECK_EQUAL(getClientHeader(body, "expect"), "100-continue");
    }

    client.sendExpect100Continue(false);

    {
        int done(false);
        string body;
        auto callbacks = std::make_shared<HttpClientSimpleCallbacks>(
            [&](const HttpRequest&, HttpClientError error,
                int statusCode, std::string&&, std::string&& newBody) {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            body = move(newBody);
            done = true;
            ML::futex_wake(done);
        });

        const std::string& bigPayload = randomString(2024);
        HttpRequest::Content content(bigPayload, "application/x-nothing");
        client.post("/post-test", callbacks, content);

        while (!done) {
            ML::futex_wait(done, false);
        }

        BOOST_CHECK_EQUAL(getClientHeader(body, "expect"), "");
    }
}
#endif

#if 1
/* Test connection restoration after a timeout occurs. */
BOOST_AUTO_TEST_CASE( test_http_client_connection_timeout )
{
    ML::Watchdog watchdog(30);
    auto proxies = make_shared<ServiceProxies>();

    HttpGetService service(proxies);
    service.addResponse("GET", "/", 200, "coucou");
    service.start();
    service.waitListening();

    string baseUrl("http://127.0.0.1:" + to_string(service.port()));

    boost::asio::io_service ioService;
    HttpClientV3 client(ioService, baseUrl, 1);

    int done(0);
    auto onDone = [&] (const HttpRequest & rq,
                       HttpClientError errorCode, int status,
                       string && headers, string && body) {
        done++;
        ML::futex_wake(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
    client.get("/timeout", cbs, {}, {}, 1);
    client.get("/", cbs, {}, {}, 1);

    while (done < 2) {
        ML::futex_wait(done, done);
    }
}
#endif

#if 1
/* Test connection restoration after the server closes the connection, under
 * various circumstances. */
BOOST_AUTO_TEST_CASE( test_http_client_connection_closed )
{
    ML::Watchdog watchdog(30);
    auto proxies = make_shared<ServiceProxies>();

    boost::asio::io_service ioService;

    HttpGetService service(proxies);
    service.portToUse = 8080;
    service.addResponse("GET", "/", 200, "coucou");
    service.start();
    service.waitListening();

    string baseUrl("http://127.0.0.1:" + to_string(service.port()));

    /* response sent, "Connection: close" header */
    {
        cerr << "* connection-close\n";
        HttpClientV3 client(ioService, baseUrl, 1);

        int done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/connection-close", cbs);
        client.get("/", cbs);

        while (done < 2) {
            ML::futex_wait(done, done);
        }
    }

    /* response sent, no "Connection: close" header */
    {
        cerr << "* no connection-close\n";
        HttpClientV3 client(ioService, baseUrl, 1);

        int done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/quiet-connection-close", cbs);
        client.get("/", cbs);

        while (done < 2) {
            ML::futex_wait(done, done);
        }
    }

    /* response not sent */
    {
        cerr << "* no response at all\n";
        HttpClientV3 client(ioService, baseUrl, 1);

        int done(0);
        auto onDone = [&] (const HttpRequest & rq,
                           HttpClientError errorCode, int status,
                           string && headers, string && body) {
            done++;
            ML::futex_wake(done);
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);
        client.get("/abrupt-connection-close", cbs);
        client.get("/", cbs);

        while (done < 2) {
            ML::futex_wait(done, done);
        }
    }
}
#endif
