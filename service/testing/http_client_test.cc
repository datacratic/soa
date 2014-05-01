#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <memory>
#include <string>
#include <tuple>
#include <boost/test/unit_test.hpp>

#include "jml/utils/testing/watchdog.h"
#include "soa/service/message_loop.h"
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
doGetRequest(MessageLoop & loop,
             const string & baseUrl, const string & resource,
             const RestParams & headers = RestParams(),
             int timeout = -1)
{
    ClientResponse response;

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
    auto client = make_shared<HttpClient>(baseUrl);
    loop.addSource("httpClient", client);
    client->waitConnectionState(AsyncEventSource::CONNECTED);
    if (timeout == -1) {
        client->get(resource, cbs, RestParams(), headers);
    }
    else {
        client->get(resource, cbs, RestParams(), headers,
                    timeout);
    }

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    loop.removeSource(client.get());
    client->waitConnectionState(AsyncEventSource::DISCONNECTED);

    return response;
}

ClientResponse
doUploadRequest(MessageLoop & loop,
                bool isPut,
                const string & baseUrl, const string & resource,
                const string & body, const string & type)
{
    ClientResponse response;
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

    auto client = make_shared<HttpClient>(baseUrl);
    loop.addSource("httpClient", client);
    client->waitConnectionState(AsyncEventSource::CONNECTED);
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    MimeContent content(body, type);
    if (isPut) {
        client->put(resource, cbs, content);
    }
    else {
        client->post(resource, cbs, content);
    }

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }
    
    loop.removeSource(client.get());
    client->waitConnectionState(AsyncEventSource::DISCONNECTED);

    return response;
}

}

#if 1
BOOST_AUTO_TEST_CASE( http_response_parser_test )
{
    string statusLine;
    vector<string> headers;
    string body;
    bool done(false);

    auto clearResult = [&] () {
        statusLine.clear();
        headers.clear();
        body.clear();
        done = true;
    };

    HttpResponseParser parser;
    parser.onResponseStart = [&] (const string & httpVersion, int code) {
        cerr << "response start\n";
        statusLine = httpVersion + "/" + to_string(code);
    };
    parser.onHeader = [&] (const char * data, size_t size) {
        // cerr << "header: " + string(data, size) + "\n";
        headers.emplace_back(data, size);
    };
    parser.onData = [&] (const char * data, size_t size) {
        // cerr << "data\n";
        body.append(data, size);
    };
    parser.onDone = [&] () {
        done = true;
    };

    auto checkTrow = [&] (const string & data) {
        BOOST_CHECK_THROW(parser.feed(data.c_str(), data.size()),
                          ML::Exception);
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
    BOOST_CHECK_EQUAL(headers.size(), 4);
    BOOST_CHECK_EQUAL(headers[3], "Content-Length: 10");
    BOOST_CHECK_EQUAL(parser.remainingBody(), 10);
    parser.feed("\n");

    /* body */
    parser.feed("0123");
    parser.feed("456");
    parser.feed("789");
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);

    clearResult();

    /* one full response and a partial one without body */
    parser.feed("HTTP/1.1 204 No content\r\n"
                "MyHeader: my value1\r\n\r\nHTTP");

    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/204");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "MyHeader: my value1");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);

    clearResult();

    parser.feed("/1.1 666 The number of the beast\r\n"
                "Header: value\r\n\r\n");
    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/666");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "Header: value");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_get )
{
    cerr << "client_get\n";
    ML::Watchdog watchdog(10);
    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    sleep (1);

    service.addResponse("GET", "/coucou", 200, "coucou");
    service.start();

    MessageLoop loop;
    loop.start();

    service.waitListening();

#if 1
    /* FIXME: this test does not work because the Datacratic router silently
     * either drops packets to unreachable host or the arp timeout is very
     * high */
    /* request to bad ip */
    {
        string baseUrl("http://123.234.12.23");
        auto resp = doGetRequest(loop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp),
                          ConnectionResult::COULD_NOT_CONNECT);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* FIXME: this test does not work because the Datacratic name service
     * always returns something */
#if 1
    /* request to bad hostname */
    {
        string baseUrl("http://somewhere.lost");
        auto resp = doGetRequest(loop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp),
                          ConnectionResult::HOST_UNKNOWN);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* request with timeout */
    {
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/timeout", {}, 1);
        BOOST_CHECK_EQUAL(get<0>(resp),
                          ConnectionResult::TIMEOUT);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }

    /* request to /nothing -> 404 */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), ConnectionResult::SUCCESS);
        BOOST_CHECK_EQUAL(get<1>(resp), 404);
    }

    /* request to /coucou -> 200 + "coucou" */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/coucou");
        BOOST_CHECK_EQUAL(get<0>(resp), ConnectionResult::SUCCESS);
        BOOST_CHECK_EQUAL(get<1>(resp), 200);
        BOOST_CHECK_EQUAL(get<2>(resp), "coucou");
    }

    /* headers and cookies */
    {
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/headers",
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

    sleep (1);

    MessageLoop loop;
    loop.start();

    /* request to /coucou -> 200 + "coucou" */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doUploadRequest(loop, false, baseUrl, "/post-test",
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

    sleep (1);

    MessageLoop loop;
    loop.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));
    string bigBody;
    for (int i = 0; i < 65535; i++) {
        bigBody += "this is one big body,";
    }
    auto resp = doUploadRequest(loop, true, baseUrl, "/put-test",
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
/* Ensures that all requests are correctly performed under load.
   Not a performance test. */
BOOST_AUTO_TEST_CASE( test_http_client_stress_test )
{
    cerr << "stress_test\n";
    // ML::Watchdog watchdog(300);
    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    service.addResponse("GET", "/", 200, "coucou");
    service.start();
    service.waitListening();

    MessageLoop loop;
    loop.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    auto client = make_shared<HttpClient>(baseUrl);
    auto & clientRef = *client.get();
    loop.addSource("httpClient", client);

    int maxReqs(10000), numReqs(0), missedReqs(0);
    int numResponses(0);

    auto onDone = [&] (const HttpRequest & rq,
                       int errorCode, int status,
                       string && headers, string && body) {
        // cerr << "* onResponse " + to_string(numResponses) + "\n";
        //          + ": " + to_string(get<1>(resp))
        //          + "\n\n\n");
        // cerr << "    body =\n/" + get<2>(resp) + "/\n";
        numResponses++;
        if ((numResponses & 0xff) == 0 || numResponses > 9980) {
            ::fprintf(stderr, "responses: %d\n", numResponses);
        }
        if (numResponses == numReqs) {
            ML::futex_wake(numResponses);
        }
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);

    while (numReqs < maxReqs) {
        if (clientRef.get("/", cbs)) {
            numReqs++;
            // if ((numReqs & 0xff) == 0 || numReqs > 9980) {
            //     cerr << "requests performed: " + to_string(numReqs) + "\n";
            // }
        }
        else {
            missedReqs++;
        }
    }
    cerr << "performed all requests: " + to_string(maxReqs) + "\n";
    cerr << " missedReqs: " + to_string(missedReqs) + "\n";

    while (numResponses < maxReqs) {
        int old(numResponses);
        ML::futex_wait(numResponses, old);
    }

    cerr << " disconnecting client\n";
    loop.removeSource(client.get());
    cerr << " removed source\n";
    client->waitConnectionState(AsyncEventSource::DISCONNECTED);
    cerr << " done\n";
}
#endif
