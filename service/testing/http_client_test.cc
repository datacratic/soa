#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <memory>
#include <string>
#include <tuple>
#include <boost/test/unit_test.hpp>

#include "jml/utils/testing/watchdog.h"
#include "soa/service/http_endpoint.h"
#include "soa/service/named_endpoint.h"
#include "soa/service/message_loop.h"
#include "soa/service/rest_proxy.h"
#include "soa/service/rest_service_endpoint.h"
#include "soa/service/http_client.h"

using namespace std;
using namespace Datacratic;

/* helpers functions used in tests */
namespace {

struct HttpTestConnHandler;

struct HttpService : public ServiceBase, public HttpEndpoint {
    HttpService(const shared_ptr<ServiceProxies> & proxies)
        : ServiceBase("http-test-service", proxies),
          HttpEndpoint("http-test-service-ep"),
          portToUse(0)
    {
    }

    ~HttpService()
    {
        shutdown();
    }

    void start()
    {
        init(portToUse, "127.0.0.1", 1);
    }

    virtual shared_ptr<ConnectionHandler> makeNewHandler();
    virtual void handleHttpPayload(HttpTestConnHandler & handler,
                                   const HttpHeader & header,
                                   const string & payload) = 0;

    int portToUse;
};

struct HttpTestConnHandler : HttpConnectionHandler {
    virtual void handleHttpPayload(const HttpHeader & header,
                                   const string & payload) {
        HttpService *svc = (HttpService *) httpEndpoint;
        svc->handleHttpPayload(*this, header, payload);
    }

    void sendResponse(int code, const string & body, const string & type)
    {
        putResponseOnWire(HttpResponse(code, type, body));
    }
};

shared_ptr<ConnectionHandler>
HttpService::
makeNewHandler()
{
    return make_shared<HttpTestConnHandler>();
}

struct HttpGetService : public HttpService {
    HttpGetService(const shared_ptr<ServiceProxies> & proxies)
        : HttpService(proxies)
    {}

    struct TestResponse {
        TestResponse(int code = 0, const string & body = "")
            : code_(code), body_(body)
        {}

        int code_;
        string body_;
    };

    void handleHttpPayload(HttpTestConnHandler & handler,
                           const HttpHeader & header,
                           const string & payload)
    {
        string key = header.verb + ":" + header.resource;
        if (header.resource == "/timeout") {
            sleep(3);
            handler.sendResponse(200, "Will time out", "text/plain");
        }
        else if (header.resource == "/headers") {
            string headersBody("{\n");
            bool first(true);
            for (const auto & it: header.headers) {
                if (first) {
                    first = false;
                }
                else {
                    headersBody += ",\n";
                }
                headersBody += "  \"" + it.first + "\": \"" + it.second + "\"\n";
            }
            headersBody += "}\n";
            handler.sendResponse(200, headersBody, "application/json");
        }
        else {
            const auto & it = responses_.find(key);
            if (it == responses_.end()) {
                handler.sendResponse(404, "Not found", "text/plain");
            }
            else {
                const TestResponse & resp = it->second;
                handler.sendResponse(resp.code_, resp.body_, "text/plain");
            }
        }
    }

    void addResponse(const string & verb, const string & resource,
                     int code, const string & body)
    {
        string key = verb + ":" + resource;
        responses_[key] = TestResponse(code, body);
    }

    map<string, TestResponse> responses_;
};

struct HttpUploadService : public HttpService {
    HttpUploadService(const shared_ptr<ServiceProxies> & proxies)
        : HttpService(proxies)
    {}

    void handleHttpPayload(HttpTestConnHandler & handler,
                           const HttpHeader & header,
                           const string & payload)
    {
        Json::Value response;

        string cType = header.contentType;
        if (cType.empty()) {
            cType = header.tryGetHeader("Content-Type");
        }
        response["verb"] = header.verb;
        response["type"] = cType;
        response["payload"] = payload;
        
        handler.sendResponse(200, response.toString(), "application/json");
    }
};

typedef tuple<HttpClientError, int, std::string> ClientResponse;

/* sync request helpers */
ClientResponse
doGetRequest(MessageLoop & loop,
             const string & baseUrl, const string & resource,
             const RestParams & headers = RestParams(),
             int timeout = -1)
{
    ClientResponse response;

    int done(false);
    auto onResponseStart = [&] (const HttpRequest & rq,
                                const std::string & httpVersion,
                                int code_) {
        int & code = get<1>(response);
        code = code_;
    };
    auto onData = [&] (const HttpRequest & rq, const std::string & data) {
        string & body = get<2>(response);
        body += data;
    };
    auto onDone = [&] (const HttpRequest & rq,
                       HttpClientError errorCode_) {
        HttpClientError & errorCode = get<0>(response);
        errorCode = errorCode_;
        done = true;
        ML::futex_wake(done);
    };

    auto cbs = make_shared<HttpClientCallbacks>(onResponseStart, nullptr,
                                                onData, onDone);

    auto client = make_shared<HttpClient>(baseUrl);
    loop.addSource("httpClient", client);
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
    HttpClientError errorCode;
    string respBody;
    int code;
    int done(false);

    auto onResponseStart = [&] (const HttpRequest & rq,
                                const std::string & httpVersion,
                                int code_) {
        code = code_;
    };
    auto onData = [&] (const HttpRequest & rq, const std::string & data) {
        respBody += data;
    };
    auto onDone = [&] (const HttpRequest & rq,
                       HttpClientError errorCode_) {
        errorCode = errorCode_;
        done = true;
        ML::futex_wake(done);
    };

    auto client = make_shared<HttpClient>(baseUrl);
    loop.addSource("httpClient", client);
    auto cbs = make_shared<HttpClientCallbacks>(onResponseStart, nullptr,
                                                onData, onDone);
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

    ClientResponse response(errorCode, code, respBody);

    return response;
}

}


#if 1
BOOST_AUTO_TEST_CASE( test_http_client_get )
{
    ML::Watchdog watchdog(10);
    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    service.addResponse("GET", "/coucou", 200, "coucou");
    service.start();

    MessageLoop loop;
    loop.start();

    /* request to bad ip */
    {
        string baseUrl("http://123.234.12.23");
        auto resp = doGetRequest(loop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp),
                          HttpClientError::COULD_NOT_CONNECT);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }

    /* FIXME: this test does not work because the Datacratic name service
     * always returns something */
#if 0
    /* request to bad hostname */
    {
        string baseUrl("http://somewhere.lost");
        auto resp = doGetRequest(loop, baseUrl, "/");
        BOOST_CHECK_EQUAL(get<0>(resp),
                          HttpClientError::HOST_NOT_FOUND);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }
#endif

    /* request with timeout */
    {
        string baseUrl("http://127.0.0.1:" + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/timeout", {}, 1);
        BOOST_CHECK_EQUAL(get<0>(resp),
                          HttpClientError::TIMEOUT);
        BOOST_CHECK_EQUAL(get<1>(resp), 0);
    }

    /* request to /nothing -> 404 */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/nothing");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::NONE);
        BOOST_CHECK_EQUAL(get<1>(resp), 404);
    }

    /* request to /coucou -> 200 + "coucou" */
    {
        string baseUrl("http://127.0.0.1:"
                       + to_string(service.port()));
        auto resp = doGetRequest(loop, baseUrl, "/coucou");
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::NONE);
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
        BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::NONE);
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
    BOOST_CHECK_EQUAL(get<0>(resp), HttpClientError::NONE);
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
    ML::Watchdog watchdog(30);
    auto proxies = make_shared<ServiceProxies>();
    HttpGetService service(proxies);

    service.addResponse("GET", "/", 200, "coucou");
    service.start();

    MessageLoop loop;
    loop.start();

    string baseUrl("http://127.0.0.1:"
                   + to_string(service.port()));

    auto client = make_shared<HttpClient>(baseUrl);
    auto & clientRef = *client.get();
    loop.addSource("httpClient", client);

    int maxReqs(10000), numReqs(0);
    int numResponses(0);

    auto onDone = [&] (const HttpRequest & rq,
                       HttpClientError errorCode, int status,
                       const string & headers, const string & body) {
        // cerr << ("* onResponse " + to_string(numResponses)
        //          + ": " + to_string(get<1>(resp))
        //          + "\n\n\n");
        // cerr << "    body =\n/" + get<2>(resp) + "/\n";
        numResponses++;
        if (numResponses == numReqs) {
            ML::futex_wake(numResponses);
        }
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onDone);

    while (numReqs < maxReqs) {
        if (clientRef.get("/", cbs)) {
            numReqs++;
        }
    }

    while (numResponses < numReqs) {
        int old(numResponses);
        ML::futex_wait(numResponses, old);
    }
}
#endif

#if 1
/* Ensure that the move constructor and assignment operator behave
   reasonably well. */
BOOST_AUTO_TEST_CASE( test_http_client_move_constructor )
{
    ML::Watchdog watchdog(30);
    auto proxies = make_shared<ServiceProxies>();

    HttpGetService service(proxies);
    service.addResponse("GET", "/", 200, "coucou");
    service.start();

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
                           const string & headers, const string & body) {
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
    HttpClient client2("notp://nowhere", 1);
    client2 = move(client1);
    doGet(client2);
}
#endif
