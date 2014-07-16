/* json_service_endpoint_test.cc
   Jeremy Barnes, 9 November 2012
   Copyright (c) 2012 Datacratic Inc.  All rights reserved.

   Test for the JSON service endpoint.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <sys/socket.h>
#include <atomic>
#include <thread>
#include <boost/test/unit_test.hpp>

#include "jml/utils/guard.h"
#include "jml/arch/exception_handler.h"
#include "jml/utils/testing/watchdog.h"
#include "jml/arch/timers.h"
#include "soa/service/http_client.h"
#include "soa/service/rest_service_endpoint.h"
#include "soa/service/rest_proxy.h"
#include "soa/service/testing/zookeeper_temporary_server.h"
#include "soa/utils/benchmarks.h"


using namespace std;
using namespace ML;
using namespace Datacratic;


/*****************************************************************************/
/* ECHO SERVICE                                                              */
/*****************************************************************************/

/** Simple test service that listens on zeromq and simply echos everything
    that it gets back.
*/

struct EchoService : public ServiceBase, public RestServiceEndpoint {

    EchoService(std::shared_ptr<ServiceProxies> proxies,
                const std::string & serviceName)
        : ServiceBase(serviceName, proxies),
          RestServiceEndpoint(proxies->zmqContext)
    {
        proxies->config->removePath(serviceName);
        RestServiceEndpoint::init(proxies->config,
                                  serviceName, 0.0005 /* maxAddedLatency */);
    }

    ~EchoService()
    {
        shutdown();
    }

    virtual void handleRequest(const ConnectionId & connection,
                               const RestRequest & request) const
    {
        //cerr << "handling request " << request << endl;
        if (request.verb != "POST")
            throw ML::Exception("echo service needs POST");
        if (request.resource != "/echo")
            throw ML::Exception("echo service only responds to /echo");
        connection.sendResponse(200, request.payload, "text/plain");
    }
};

#if 0
BOOST_AUTO_TEST_CASE( test_named_endpoint )
{
    ZooKeeper::TemporaryServer zookeeper;
    zookeeper.start();

    auto proxies = std::make_shared<ServiceProxies>();
    proxies->useZookeeper(ML::format("localhost:%d", zookeeper.getPort()));

    int totalPings = 1000;

    EchoService service(proxies, "echo");
    auto addr = service.bindTcp();
    cerr << "echo service is listening on " << addr.first << " and "
         << addr.second << endl;

    service.start();

    proxies->config->dump(cerr);


    volatile int numPings = 0;

    auto runZmqThread = [=, &numPings] ()
        {
            RestProxy proxy(proxies->zmqContext);
            proxy.init(proxies->config, "echo");
            proxy.start();
            cerr << "connected" << endl;

            volatile int numOutstanding = 0;

            while (numPings < totalPings) {
                int i = __sync_add_and_fetch(&numPings, 1);

                if (i && i % 1000 == 0)
                    cerr << i << " with " << numOutstanding << " outstanding"
                         << endl;

                auto onResponse = [=, &numOutstanding]
                    (std::exception_ptr ptr,
                     int responseCode,
                     std::string body)
                    {
                        //cerr << "got response " << responseCode
                        //     << endl;
                        ML::atomic_dec(numOutstanding);

                        if (ptr)
                            throw ML::Exception("response returned exception");
                        ExcAssertEqual(responseCode, 200);
                        ExcAssertEqual(body, to_string(i));

                        futex_wake(numOutstanding);
                    };
                
                proxy.push(onResponse,
                           "POST", "/echo", {}, to_string(i));
                ML::atomic_inc(numOutstanding);
            }

            proxy.sleepUntilIdle();

            //ML::sleep(1.0);

            cerr << "shutting down proxy " << this << endl;
            proxy.shutdown();
            cerr << "done proxy shutdown" << endl;
        };

#if 0
    auto runHttpThread = [&] ()
        {
            HttpNamedRestProxy proxy;
            proxy.init(proxies->config);
            proxy.connect("echo/http");

            while (numPings < totalPings) {
                int i = __sync_add_and_fetch(&numPings, 1);

                if (i && i % 1000 == 0)
                    cerr << i << endl;

                auto response = proxy.post("/echo", to_string(i));
                
                ExcAssertEqual(response.code_, 200);
                ExcAssertEqual(response.body_, to_string(i));
            }

        };
#endif

    boost::thread_group threads;

    for (unsigned i = 0;  i < 8;  ++i) {
        threads.create_thread(runZmqThread);
    }

    //for (unsigned i = 0;  i < 5;  ++i) {
    //    threads.create_thread(runHttpThread);
    //}

    threads.join_all();

    cerr << "finished requests" << endl;

    service.shutdown();
}
#endif

#if 1
BOOST_AUTO_TEST_CASE( bench_named_endpoint )
{
    static const int totalPings(1000);

    Benchmarks bm;

    
    ZooKeeper::TemporaryServer zookeeper;
    zookeeper.start();

    auto proxies = make_shared<ServiceProxies>();
    proxies->useZookeeper(ML::format("localhost:%d", zookeeper.getPort()));

    EchoService service(proxies, "echo");
    auto addr = service.bindTcp();
    cerr << "echo service is listening on " << addr.first << " and "
         << addr.second << endl;

    service.start();

    proxies->config->dump(cerr);

    int numPings;
    atomic<int> numOutstandings;

#if 0
    /* ZMQ test */
    {
        RestProxy proxy(proxies->zmqContext);
        proxy.init(proxies->config, "echo");
        proxy.start();

        shared_ptr<Benchmark> zmqBm(new Benchmark(bm, "zmq-threads"));

        for (numPings = 0; numPings < totalPings; numPings++) {
            auto onResponse = [&] (exception_ptr ptr,
                                   int responseCode,
                                   string body) {
                ExcAssertEqual(responseCode, 200);
                numOutstandings--;
            };
            proxy.push(onResponse,
                       "POST", "/echo", {}, to_string(numPings));
            numOutstandings++;
        }

        while (numOutstandings > 0) {
            ML::sleep(0.1);
        }
        zmqBm.reset();

        proxy.shutdown();
    }
#endif

    /* HTTP test */
    {
        Json::Value value = proxies->config->getJson("echo/http/tcp");
        string echoUrl = value[0]["httpUri"].asString();
        if (echoUrl.empty()) {
            throw ML::Exception("no service url");
        }
        cerr << "  service url: "  + echoUrl + "\n";

        MessageLoop loop;

        loop.start();

        auto client = make_shared<HttpClient>(echoUrl);
        loop.addSource("client", client);
        client->waitConnectionState(AsyncEventSource::CONNECTED);

        auto onResponse = [&] (const HttpRequest & rq,
                               HttpClientError error,
                               int status,
                               string && headers,
                               string && body) {
            ExcAssertEqual(status, 200);
            numOutstandings--;
        };
        auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
        
        shared_ptr<Benchmark> httpBm(new Benchmark(bm, "http-threads"));
        string bodyType("text/plain");
        for (numPings = 0; numPings < totalPings; numPings++) {
            string body = to_string(numPings);
            HttpRequest::Content content(body, bodyType);
            client->post("/echo", cbs, content);
            numOutstandings++;
        }

        while (numOutstandings > 0) {
            ML::sleep(0.1);
        }
        httpBm.reset();
    
        loop.removeSource(client.get());
        client->waitConnectionState(AsyncEventSource::DISCONNECTED);
    }

    bm.dumpTotals();

    cerr << "finished requests" << endl;

    service.shutdown();
}
#endif
