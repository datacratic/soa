#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "googleurl/src/gurl.h"
#include "jml/arch/timers.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/guard.h"
#include "soa/types/url.h"

#include "boost/asio/error.hpp"

#include "asio_http_service.h"

using namespace std;
using namespace boost;
using namespace Datacratic;


/****************************************************************************/
/* ASIO HTTP HANDLER                                                        */
/****************************************************************************/

AsioHttpHandler::
AsioHttpHandler(asio::ip::tcp::socket && socket)
    : AsioTcpHandler(std::move(socket))
{
    parser_.onRequestStart = [&] (const char * methodData, size_t methodSize,
                                  const char * urlData, size_t urlSize,
                                  const char * versionData,
                                  size_t versionSize) {
        this->onRequestStart(methodData, methodSize, urlData, urlSize,
                             versionData, versionSize);
    };
    parser_.onHeader = [&] (const char * data, size_t dataSize) {
        this->onHeader(data, dataSize);
    };
    parser_.onData = [&] (const char * data, size_t dataSize) {
        this->onData(data, dataSize);
    };
    parser_.onDone = [&] (bool shouldClose) {
        this->onDone(shouldClose);
    };
}

void
AsioHttpHandler::
bootstrap()
{
    requestReceive();
}

void
AsioHttpHandler::
onReceivedData(const char * data, size_t size)
{
    try {
        parser_.feed(data, size);
        requestReceive();
    }
    catch (const ML::Exception & exc) {
        requestClose();
    }
}

void
AsioHttpHandler::
onReceiveError(const system::error_code & ec, size_t bufferSize)
{
    if (ec == system::errc::connection_reset || ec == asio::error::eof) {
        detach();
    }
    else {
        throw ML::Exception("unhandled error: " + ec.message());
    }
}


/****************************************************************************/
/* HTTP CLASSIC HANDLER                                                     */
/****************************************************************************/

AsioHttpClassicHandler::
AsioHttpClassicHandler(asio::ip::tcp::socket && socket)
    : AsioHttpHandler(std::move(socket)), bodyStarted_(false)
{
}

void
AsioHttpClassicHandler::
send(const std::string & str,
     PassiveConnectionHandler::NextAction action,
     PassiveConnectionHandler::OnWriteFinished onWriteFinished)
{
    if (str.size() > 0) {
        auto onWritten = [=] (const boost::system::error_code & ec,
                              size_t) {
            if (onWriteFinished) {
                onWriteFinished();
            }
            if (action == PassiveConnectionHandler::NEXT_CLOSE
                || action == PassiveConnectionHandler::NEXT_RECYCLE) {
                requestClose();
            }
        };
        requestWrite(str, onWritten);
    }
}

void
AsioHttpClassicHandler::
putResponseOnWire(const HttpResponse & response,
                  std::function<void ()> onSendFinished,
                  PassiveConnectionHandler::NextAction next)
{
    string responseStr;
    responseStr.reserve(1024 + response.body.length());

    responseStr.append("HTTP/1.1 ");
    responseStr.append(to_string(response.responseCode));
    responseStr.append(" ");
    responseStr.append(response.responseStatus);
    responseStr.append("\r\n");

    if (response.contentType != "") {
        responseStr.append("Content-Type: ");
        responseStr.append(response.contentType);
        responseStr.append("\r\n");
    }

    if (response.sendBody) {
        responseStr.append("Content-Length: ");
        responseStr.append(to_string(response.body.length()));
        responseStr.append("\r\n");
        responseStr.append("Connection: Keep-Alive\r\n");
    }

    for (auto & h: response.extraHeaders) {
        responseStr.append(h.first);
        responseStr.append(": ");
        responseStr.append(h.second);
        responseStr.append("\r\n");
    }

    responseStr.append("\r\n");
    responseStr.append(response.body);

    //cerr << "sending " << responseStr << endl;
    
    send(responseStr, next, onSendFinished);
}

void
AsioHttpClassicHandler::
onRequestStart(const char * methodData, size_t methodSize,
               const char * urlData, size_t urlSize,
               const char * versionData, size_t versionSize)
{
    headerPayload.reserve(8192);
    headerPayload.append(methodData, methodSize);
    headerPayload.append(" ", 1);
    headerPayload.append(urlData, urlSize);
    headerPayload.append(" ", 1);
    headerPayload.append(versionData, versionSize);
    headerPayload.append("\r\n", 2);
}

void
AsioHttpClassicHandler::
onHeader(const char * data, size_t size)
{
    headerPayload.append(data, size);
}

void
AsioHttpClassicHandler::
onData(const char * data, size_t size)
{
    if (!bodyStarted_) {
        bodyStarted_ = true;
    }
    bodyPayload.append(data, size);
}

void
AsioHttpClassicHandler::
onDone(bool requireClose)
{
    HttpHeader header;
    // cerr << "payload to parse: " + payload + "\n";
    header.parse(headerPayload);
    handleHttpPayload(header, bodyPayload);
    headerPayload.clear();
    bodyPayload.clear();
    bodyStarted_ = false;
}
