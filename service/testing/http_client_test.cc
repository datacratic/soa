#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <memory>
#include <string>
#include <tuple>
#include <boost/test/unit_test.hpp>

#include "jml/arch/futex.h"
#include "jml/arch/timers.h"
#include "jml/utils/testing/watchdog.h"
#include "soa/service/message_loop.h"
#include "soa/service/rest_proxy.h"
#include "soa/service/http_client.h"
#include "soa/utils/print_utils.h"

#include "test_http_services.h"


using namespace std;
using namespace Datacratic;


/* helpers functions used in tests */
namespace {

typedef tuple<HttpClientError, int, string> ClientResponse;

#define CALL_MEMBER_FN(object, pointer)  ((object)->*(pointer))

/* sync request helpers */
template<typename Func>
ClientResponse
doRequest(MessageLoop& loop, const string &baseUrl, const string& resource,
          Func func,
          const RestParams &headers = RestParams(),
          int timeout = -1)
{
    ClientResponse response;

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
    auto client = make_shared<HttpClient>(baseUrl, 4);
    HttpClient *ptr = client.get();
    loop.addSource("httpClient", client);
    client->waitConnectionState(AsyncEventSource::CONNECTED);

    CALL_MEMBER_FN(ptr, func)(resource, cbs, RestParams(), headers,
                timeout);

    while (!done) {
        int oldDone = done;
        ML::futex_wait(done, oldDone);
    }

    loop.removeSource(ptr);
    client->waitConnectionState(AsyncEventSource::DISCONNECTED);

    return response;
}

ClientResponse
doGetRequest(MessageLoop & loop,
             const string & baseUrl, const string & resource,
             const RestParams & headers = RestParams(),
             int timeout = -1)
{
    return doRequest(loop, baseUrl, resource,
              &HttpClient::get,
              headers,
              timeout);
}

ClientResponse
doDeleteRequest(MessageLoop & loop,
               const string & baseUrl, const string & resource,
               const RestParams & headers = RestParams(),
               int timeout = -1)
{
    return doRequest(loop, baseUrl, resource,
              &HttpClient::del,
              headers,
              timeout);
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

    auto client = make_shared<HttpClient>(baseUrl, 4);
    loop.addSource("httpClient", client);
    client->waitConnectionState(AsyncEventSource::CONNECTED);
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    HttpRequest::Content content(body, type);
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
BOOST_AUTO_TEST_CASE( test_http_client_get )
{
    cerr << "client_get\n";
    ML::Watchdog watchdog(10);
    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    service.addResponse("GET", "/coucou", 200, "coucou");
    service.start();

    MessageLoop loop;
    loop.start();

    service.waitListening();

#if 0
    /* FIXME: this test does not work because the Datacratic router silently
     * either drops packets to unreachable host or the arp timeout is very
     * high */
    /* request to bad ip */
    {
        string baseUrl("http://123.234.12.23");
        auto resp = doGetRequest(loop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp),
                          HttpClientError::COULD_NOT_CONNECT);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* FIXME: this test does not work because the Datacratic name service
     * always returns something */
#if 0
    /* request to bad hostname */
    {
        string baseUrl("http://somewhere.lost");
        auto resp = doGetRequest(loop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp),
                          HttpClientError::HostNotFound);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* request with timeout */
    {
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/timeout", {}, 1);
        BOOST_CHECK_EQUAL(get<0>(resp),
                          HttpClientError::Timeout);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }

    /* request to /nothing -> 404 */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
        BOOST_CHECK_EQUAL(get<1>(resp), 404);
    }

    /* request to /coucou -> 200 + "coucou" */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/coucou");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
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

    MessageLoop loop;
    loop.start();

    /* request to /coucou -> 200 + "coucou" */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doUploadRequest(loop, false, baseUrl, "/post-test",
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

#if 1
BOOST_AUTO_TEST_CASE( test_http_client_put )
{
    cerr << "client_put\n";
    ML::Watchdog watchdog(10);
    auto proxies = make_shared<ServiceProxies>();
    HttpUploadService service(proxies);
    service.start();

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
    BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
    BOOST_CHECK_EQUAL(get<1>(resp), 200);
    Json::Value jsonBody = Json::parse(get<2>(resp));
    BOOST_CHECK_EQUAL(jsonBody["verb"], "PUT");
    BOOST_CHECK_EQUAL(jsonBody["payload"], bigBody);
    BOOST_CHECK_EQUAL(jsonBody["type"], "application/x-nothing");
}
#endif

BOOST_AUTO_TEST_CASE( http_test_client_delete )
{
    cerr << "client_delete" << endl;
    ML::Watchdog watchdog(10);

    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    service.addResponse("DELETE", "/deleteMe", 200, "Deleted");
    service.start();

    MessageLoop loop;
    loop.start();

    string baseUrl("http://127.0.0.1:" + to_string(service.port()));
    auto resp = doDeleteRequest(loop, baseUrl, "/deleteMe", {}, 1);

    BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::None);
    BOOST_CHECK_EQUAL(get<1>(resp), 200);
}

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

    string baseUrl("http://127.0.0.1:" + to_string(service.port()));

    auto client = make_shared<HttpClient>(baseUrl, 4);
    auto & clientRef = *client.get();
    loop.addSource("httpClient", client);

    int maxReqs(10000), numReqs(0), missedReqs(0);
    int numResponses(0);

    auto onDone = [&] (const HttpRequest & rq,
                       HttpClientError errorCode, int status,
                       string && headers, string && body) {
        // cerr << ("* onResponse " + to_string(numResponses)
        //          + ": " + to_string(get<1>(resp))
        //          + "\n\n\n");
        // cerr << "    body =\n/" + get<2>(resp) + "/\n";
        numResponses++;
        // if ((numResponses & 0xff) == 0 || numResponses > 9980) {
        //     ::fprintf(stderr, "responses: %d\n", numResponses);
        // }
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

    loop.removeSource(client.get());
    client->waitConnectionState(AsyncEventSource::DISCONNECTED);
}
#endif

#if 1
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

    MessageLoop loop;
    loop.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    auto doGet = [&] (HttpClient & getClient) {
        loop.addSource("client", getClient);
        getClient.waitConnectionState(AsyncEventSource::CONNECTED);

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

        loop.removeSource(&getClient);
        getClient.waitConnectionState(AsyncEventSource::DISCONNECTED);
    };

    /* move constructor */
    cerr << "testing move constructor\n";
    auto makeClient = [&] () {
        return HttpClient(baseUrl, 1);
    };
    HttpClient client1(move(makeClient()));
    doGet(client1);

    /* move assignment operator */
    cerr << "testing move assignment op.\n";
    HttpClient client2("http://nowhere", 1);
    client2 = move(client1);
    doGet(client2);

    service.shutdown();
}
#endif

#if 1
/* Ensure that an infinite number of requests can be queued, even from within
 * callbacks. */
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

    auto client = make_shared<HttpClient>(baseUrl, 4);
    loop.addSource("client", client);
    client->waitConnectionState(AsyncEventSource::CONNECTED);

    atomic<int> pending(0);
    int done(0);

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
        client->get("/", cbs);
    };

    doGet(0);

    while (pending > 0) {
        ML::sleep(1);
        cerr << "requests done: " + to_string(done) + "\n";
    }

    loop.removeSource(client.get());
    client->waitConnectionState(AsyncEventSource::DISCONNECTED);

    service.shutdown();
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

    auto client = make_shared<HttpClient>(baseUrl, 4);
    client->debug(true);
    client->sendExpect100Continue(true);

    MessageLoop loop;
    loop.addSource("HttpClient", client);
    loop.start();

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
                 std::string && newBody)
        {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            BOOST_CHECK_EQUAL(statusCode, 200);
            body = move(newBody);
            done = true;
            ML::futex_wake(done);
        });

        const std::string& smallPayload = randomString(20);
        HttpRequest::Content content(smallPayload, "application/x-nothing");
        client->post("/post-test", callbacks, content);

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
                    std::string&& newBody)
        {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            body = move(newBody);
            done = true;
            ML::futex_wake(done);
        });

        const std::string& bigPayload = randomString(2024);
        HttpRequest::Content content(bigPayload, "application/x-nothing");
        client->post("/post-test", callbacks, content);

        while (!done) {
            ML::futex_wait(done, false);
        }

        BOOST_CHECK_EQUAL(getClientHeader(body, "expect"), "100-continue");
    }

    client->sendExpect100Continue(false);

    {
        int done(false);
        string body;
        auto callbacks = std::make_shared<HttpClientSimpleCallbacks>(
                [&](const HttpRequest&, HttpClientError error,
                    int statusCode, std::string&&, std::string&& newBody)
        {
            BOOST_CHECK_EQUAL(error, HttpClientError::None);
            body = move(newBody);
            done = true;
            ML::futex_wake(done);
        });

        const std::string& bigPayload = randomString(2024);
        HttpRequest::Content content(bigPayload, "application/x-nothing");
        client->post("/post-test", callbacks, content);

        while (!done) {
            ML::futex_wait(done, false);
        }

        BOOST_CHECK_EQUAL(getClientHeader(body, "expect"), "");
    }

    service.shutdown();
}
#endif

BOOST_AUTO_TEST_CASE( test_synchronous_requests )
{
    ML::Watchdog watchdog(10);
    cerr << "client_expect_100_continue" << endl;

    auto proxies = make_shared<ServiceProxies>();

    HttpGetService service(proxies);
    service.addResponse("GET", "/hello", 200, "world");
    service.addResponse("POST", "/foo", 200, "bar");
    service.addResponse("PUT", "/ying", 200, "yang");
    service.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    auto client = make_shared<HttpClient>(baseUrl, 4);

    MessageLoop loop;
    loop.addSource("HttpClient", client);
    loop.start();

    {
        auto resp = client->getSync("/hello");
        BOOST_CHECK_EQUAL(resp.code_, 200);
    }

    {
        auto resp = client->getSync("/not-found");
        BOOST_CHECK_EQUAL(resp.code_, 404);
    }

    {
        auto resp = client->postSync("/foo");
        BOOST_CHECK_EQUAL(resp.code_, 200);
    }

    {
        auto resp = client->putSync("/ying");
        BOOST_CHECK_EQUAL(resp.code_, 200);
    }
}
