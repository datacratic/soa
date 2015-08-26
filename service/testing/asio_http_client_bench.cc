#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "jml/arch/exception.h"
#include "soa/types/date.h"
#include "soa/types/value_description.h"
#include "soa/utils/print_utils.h"
#include "soa/service/asio_http_client.h"
#include "soa/service/http_endpoint.h"
#include "soa/service/named_endpoint.h"
#include "soa/service/rest_proxy.h"
#include "soa/service/rest_service_endpoint.h"
#include "soa/service/runner.h"

#include "test_http_services.h"

using namespace std;
using namespace Datacratic;

enum HttpMethod {
    GET,
    POST,
    PUT
};

/* bench methods */

double
ClientBench(HttpMethod method,
            const string & baseUrl, const string & payload,
            int maxReqs, int concurrency)
{
    int numReqs, numResponses(0), numMissed(0);

    boost::asio::io_service ioService;

    AsioHttpClient client(ioService, baseUrl, concurrency);

    auto onResponse = [&] (const HttpRequest & rq, HttpClientError errorCode_,
                           int status, string && headers, string && body) {
        numResponses++;
        // if (numResponses % 1000) {
            // cerr << "resps: "  + to_string(numResponses) + "\n";
        // }
        if (numResponses == maxReqs) {
            // cerr << "received all responses\n";
            ioService.stop();
        }
    };
    auto cbs = make_shared<HttpClientSimpleCallbacks>(onResponse);
    HttpRequest::Content content(payload, "application/binary");

    string url("/");
    Date start = Date::now();
    for (numReqs = 0; numReqs < maxReqs;) {
        bool result;
        if (method == GET) {
            result = client.get(url, cbs);
        }
        else if (method == POST) {
            result = client.post(url, cbs, content);
        }
        else if (method == PUT) {
            result = client.put(url, cbs, content);
        }
        else {
            result = true;
        }
        if (result) {
            numReqs++;
            // if (numReqs % 1000) {
            //     cerr << "reqs: "  + to_string(numReqs) + "\n";
            // }
        }
        else {
            numMissed++;
        }
    }

    ioService.run();
    ExcAssertEqual(numResponses, maxReqs);
    Date end = Date::now();

    cerr << "num misses: " + to_string(numMissed) + "\n";

    return end - start;
}

int main(int argc, char *argv[])
{
    using namespace boost::program_options;

    unsigned int concurrency(0);
    unsigned int serverConcurrency(0);
    unsigned int maxReqs(0);
    string method("GET");
    unsigned int payloadSize(0);

    string serveriface("127.0.0.1");
    string clientiface(serveriface);

    options_description all_opt;
    all_opt.add_options()
        ("client-iface,C", value(&clientiface),
         "address:port to connect to (\"none\" for no client)")
        ("concurrency,c", value(&concurrency),
         "Number of concurrent requests")
        ("server-concurrency", value(&serverConcurrency),
         "Number of server worker threads (defaults to \"concurrency\")")
        ("method,M", value(&method),
         "Method to use (\"GET\"*, \"PUT\", \"POST\")")
        ("requests,r", value(&maxReqs),
         "total of number of requests to perform")
        ("payload-size,s", value(&payloadSize),
         "size of the response body")
        ("server-iface,S", value(&serveriface),
         "server address (\"none\" for no server)")
        ("help,H", "show help");

    if (argc == 1) {
        return 0;
    }

    variables_map vm;
    store(command_line_parser(argc, argv)
          .options(all_opt)
          .run(),
          vm);
    notify(vm);

    if (vm.count("help")) {
        cerr << all_opt << endl;
        return 1;
    }

    /* service setup */
    auto proxies = make_shared<ServiceProxies>();

    HttpGetService service(proxies);

    if (concurrency == 0) {
        throw ML::Exception("'concurrency' must be specified");
    }
    if (serverConcurrency == 0) {
        serverConcurrency = concurrency;
    }

    if (payloadSize == 0) {
        throw ML::Exception("'payload-size' must be specified");
    }

    string payload;
    while (payload.size() < payloadSize) {
        payload += randomString(128);
    }

    if (serveriface != "none") {
        cerr << "launching server\n";
        service.portToUse = 20000;

        service.addResponse("GET", "/", 200, payload);
        service.addResponse("PUT", "/", 200, "");
        service.addResponse("POST", "/", 200, "");
        service.start(serveriface, serverConcurrency);
    }

    if (clientiface != "none") {
        cerr << "launching client\n";
        if (maxReqs == 0) {
            throw ML::Exception("'max-reqs' must be specified");
        }

        if (!(method == "GET" || method == "POST" || method == "PUT")) {
            throw ML::Exception("invalid method:" + method);
        }

        string baseUrl;
        if (serveriface != "none") {
            baseUrl = ("http://" + serveriface
                       + ":" + to_string(service.port()));
        }
        else {
            baseUrl = "http://" + clientiface;
        }

        ::printf("conc.\treqs\tsize\ttime_secs\tBps\tqps\n");

        HttpMethod httpMethod;
        if (method == "GET") {
            httpMethod = GET;
        }
        else if (method == "POST") {
            httpMethod = POST;
        }
        else if (method == "PUT") {
            httpMethod = PUT;
        }
        else {
            throw ML::Exception("unknown method: "  + method);
        }

        double delta = ClientBench(httpMethod, baseUrl, payload, maxReqs, concurrency);
        double qps = maxReqs / delta;
        double bps = double(maxReqs * payload.size()) / delta;
        ::printf("%u\t%u\t%u\t%f\t%f\t%f\n",
                 concurrency, maxReqs, payloadSize, delta, bps, qps);
    }
    else {
        while (1) {
            sleep(100);
        }
    }

    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    Json::Value jsonUsage = jsonEncode(usage);
    cerr << "rusage:\n" << jsonUsage.toStyledString() << endl;

    return 0;
}
