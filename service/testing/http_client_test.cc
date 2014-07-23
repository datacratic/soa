#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <memory>
#include <string>
#include <tuple>
#include <boost/test/unit_test.hpp>

#include "jml/utils/testing/watchdog.h"
#include "soa/service/rest_proxy.h"
#include "soa/service/http_client.h"

#include "test_http_services.h"


using namespace std;
using namespace Datacratic;


/* helpers functions used in tests */
namespace {

typedef tuple<int, int, string> ClientResponse;

/* sync request helpers */
ClientResponse
doGetRequest(const string & baseUrl, const string & resource,
             const RestParams & headers = RestParams(),
             int timeout = -1)
{
    ClientResponse response;

    HttpClient client(baseUrl);
    client.start();

    int done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           int error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        int & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        ML::futex_wake(done);
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);

    if (timeout == -1) {
        client.get(resource, cbs, RestParams(), headers);
    }
    else {
        client.get(resource, cbs, RestParams(), headers,
                   timeout);
    }

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    return response;
}

ClientResponse
doUploadRequest(bool isPut,
                const string & baseUrl, const string & resource,
                const string & body, const string & type)
{
    ClientResponse response;

    HttpClient client(baseUrl);
    client.start();

    int done(false);
    auto onResponse = [&] (const HttpRequest & rq,
                           int error,
                           int status,
                           string && headers,
                           string && body) {
        int & code = get<1>(response);
        code = status;
        string & body_ = get<2>(response);
        body_ = move(body);
        int & errorCode = get<0>(response);
        errorCode = error;
        done = true;
        ML::futex_wake(done);
    };

    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    MimeContent content(body, type);
    if (isPut) {
        client.put(resource, cbs, content);
    }
    else {
        client.post(resource, cbs, content);
    }

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }
    
    return response;
}

}

#if 1
BOOST_AUTO_TEST_CASE( http_response_parser_test )
{
    string statusLine;
    vector<string> headers;
    string body;
    bool done;
    bool shouldClose;

    HttpResponseParser parser;
    parser.onResponseStart = [&] (const string & httpVersion, int code) {
        cerr << "response start\n";
        statusLine = httpVersion + "/" + to_string(code);
        headers.clear();
        body.clear();
        shouldClose = false;
        done = false;
    };
    parser.onHeader = [&] (const char * data, size_t size) {
        // cerr << "header: " + string(data, size) + "\n";
        headers.emplace_back(data, size);
    };
    parser.onData = [&] (const char * data, size_t size) {
        // cerr << "data\n";
        body.append(data, size);
    };
    parser.onDone = [&] (bool doClose) {
        shouldClose = doClose;
        done = true;
    };

    /* status line */
    parser.feed("HTTP/1.");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("1 200 Th");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("is is ");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("some blabla\r");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("\n");
    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/200");

    /* headers */
    parser.feed("Head");
    BOOST_CHECK_EQUAL(headers.size(), 0);
    parser.feed("er1: value1\r\nHeader2: value2");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "Header1: value1");
    parser.feed("\r");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    parser.feed("\n");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    BOOST_CHECK_EQUAL(headers[1], "Header2: value2");
    parser.feed("");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    parser.feed("He");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    parser.feed("ad");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    parser.feed("er3: Val3\r\n");
    BOOST_CHECK_EQUAL(headers.size(), 3);
    BOOST_CHECK_EQUAL(headers[2], "Header3: Val3");
    parser.feed("Content-Length: 10\r\n\r");
    parser.feed("\n");
    BOOST_CHECK_EQUAL(headers.size(), 4);
    BOOST_CHECK_EQUAL(headers[3], "Content-Length: 10");
    BOOST_CHECK_EQUAL(parser.remainingBody(), 10);

    /* body */
    parser.feed("0123");
    parser.feed("456");
    parser.feed("789");
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);

    /* one full response and a partial one without body */
    parser.feed("HTTP/1.1 204 No content\r\n"
                "MyHeader: my value1\r\n\r\nHTTP");

    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/204");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "MyHeader: my value1");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);

    parser.feed("/1.1 666 The number of the beast\r\n"
                "Connection: close\r\n"
                "Header: value\r\n\r\n");
    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/666");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    BOOST_CHECK_EQUAL(headers[0], "Connection: close");
    BOOST_CHECK_EQUAL(headers[1], "Header: value");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(shouldClose, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);

    /* 2 full reponses with body */
    const char * payload = ("HTTP/1.1 200 This is some blabla\r\n"
                            "Header1: value1\r\n"
                            "Header2: value2\r\n"
                            "Content-Type: text/plain\r\n"
                            "Content-Length: 10\r\n"
                            "\r\n"
                            "0123456789");
    parser.feed(payload);
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);
    parser.feed(payload);
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(shouldClose, false);
}
#endif

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
        BOOST_CHECK_EQUAL(get<0>(resp),
                          ConnectionResult::COULD_NOT_CONNECT);
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
        BOOST_CHECK_EQUAL(get<0>(resp),
                          ConnectionResult::HOST_UNKNOWN);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* request with timeout */
    {
        ::fprintf(stderr, "request with timeout\n");
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/timeout", {}, 1);
        BOOST_CHECK_EQUAL(get<0>(resp),
                          ConnectionResult::TIMEOUT);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }

    /* request connection close  */
    {
        ::fprintf(stderr, "testing behaviour with connection: close\n");
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/connection-close");
        BOOST_CHECK_EQUAL(get<0>(resp), ConnectionResult::SUCCESS);
        BOOST_CHECK_EQUAL(get<1>(resp), 204);
    }

    /* request to /nothing -> 404 */
    {
        ::fprintf(stderr, "request with 404\n");
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), ConnectionResult::SUCCESS);
        BOOST_CHECK_EQUAL(get<1>(resp), 404);
    }

    /* request to /coucou -> 200 + "coucou" */
    {
        ::fprintf(stderr, "request with 200\n");
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/coucou");
        BOOST_CHECK_EQUAL(get<0>(resp), ConnectionResult::SUCCESS);
        BOOST_CHECK_EQUAL(get<1>(resp), 200);
        BOOST_CHECK_EQUAL(get<2>(resp), "coucou");
    }

    /* headers and cookies */
    {
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(baseUrl, "/headers",
                                 {{"someheader", "somevalue"}});
        Json::Value expBody;
        expBody["accept"] = "*/*";
        expBody["host"] = baseUrl.substr(7);
        expBody["someheader"] = "somevalue";
        Json::Value jsonBody = Json::parse(get<2>(resp));
        BOOST_CHECK_EQUAL(jsonBody, expBody);
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
        BOOST_CHECK_EQUAL(get<0>(resp), ConnectionResult::SUCCESS);
        BOOST_CHECK_EQUAL(get<1>(resp), 200);
        Json::Value jsonBody = Json::parse(get<2>(resp));
        BOOST_CHECK_EQUAL(jsonBody["verb"], "POST");
        BOOST_CHECK_EQUAL(jsonBody["payload"], "post body");
        BOOST_CHECK_EQUAL(jsonBody["type"], "application/x-nothing");
    }
}
#endif

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
    BOOST_CHECK_EQUAL(get<0>(resp), ConnectionResult::SUCCESS);
    BOOST_CHECK_EQUAL(get<1>(resp), 200);
    Json::Value jsonBody = Json::parse(get<2>(resp));
    BOOST_CHECK_EQUAL(jsonBody["verb"], "PUT");
    BOOST_CHECK_EQUAL(jsonBody["payload"], bigBody);
    BOOST_CHECK_EQUAL(jsonBody["type"], "application/x-nothing");
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

        HttpClient client(baseUrl, numParallel);
        client.start();

        int maxReqs(30000), numReqs(0), missedReqs(0);
        int numResponses(0);

        auto onDone = [&] (const HttpRequest & rq,
                           int errorCode, int status,
                           string && headers, string && body) {
            numResponses++;

            BOOST_CHECK_EQUAL(errorCode, 0);
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
        ::fprintf(stderr, "performed %d requests; missed: %d\n",
                  maxReqs, missedReqs);
    };

    doStressTest(1);
    doStressTest(8);
    doStressTest(128);
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

    MessageLoop loop;
    loop.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    auto client = make_shared<HttpClient>(baseUrl, 4, 0);
    loop.addSource("client", client);
    client->waitConnectionState(AsyncEventSource::CONNECTED);

    atomic<int> pending(0);
    int done(0);

    function<void(int)> doGet = [&] (int level) {
        pending++;
        auto onDone = [&,level] (const HttpRequest & rq,
                                 int errorCode, int status,
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
        client->get("/", cbs);
    };

    doGet(0);

    while (pending > 0) {
        ML::sleep(1);
        cerr << "requests done: " + to_string(done) + "\n";
    }

    loop.removeSource(client.get());
    client->waitConnectionState(AsyncEventSource::DISCONNECTED);
}
#endif
