#include <iostream>
#include <string>

#include "jml/utils/exc_assert.h"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "soa/service/asio_tcp_service.h"
#include "soa/service/asio_http_service.h"


using namespace std;
using namespace boost;
using namespace Datacratic;

struct MyHandler : public AsioHttpClassicHandler {
    MyHandler(boost::asio::ip::tcp::socket && socket);

    virtual void handleHttpPayload(const HttpHeader & header,
                                   const std::string & payload);
};

MyHandler::
MyHandler(boost::asio::ip::tcp::socket && socket)
    : AsioHttpClassicHandler(std::move(socket))
{
}

void
MyHandler::
handleHttpPayload(const HttpHeader & header,
                  const std::string & payload)
{
    static string responseStr("HTTP/1.1 200 OK\r\n"
                              "Content-Type: text/plain\r\n"
                              "Content-Length: 2\r\n"
                              "\r\n"
                              "OK");
    send(responseStr);
}

int
main(int argc, char * argv[])
{
    using namespace boost::program_options;
    unsigned int concurrency(0);
    unsigned int port(20000);

    options_description all_opt;
    all_opt.add_options()
        ("concurrency,c", value(&concurrency),
         "Number of concurrent requests (mandatory)")
        ("port,p", value(&port),
         "port to listen on (20000)")
        ("help,H", "show help");

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

    ExcAssert(port > 0);
    ExcAssert(concurrency > 0);

    asio::io_service ioService;

    auto onNewConnection = [&] (asio::ip::tcp::socket && socket) {
        return std::make_shared<MyHandler>(std::move(socket));
    };

    AsioTcpAcceptor acceptor(ioService, "", port, onNewConnection, concurrency);
    cerr << "bootstrap...\n";
    acceptor.bootstrap();

    while (true) {
        cerr << "loop\n";
        ioService.run();
    }
}
