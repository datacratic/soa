#pragma once

#include "asio_tcp_service.h"
#include "http_endpoint.h"
#include "http_parsers.h"


namespace Datacratic {

struct AsioHttpHandler : public AsioTcpHandler {
    AsioHttpHandler(boost::asio::ip::tcp::socket && socket);

    virtual void bootstrap();
    virtual void onReceivedData(const char * buffer, size_t bufferSize);
    virtual void onReceiveError(const boost::system::error_code & ec,
                                size_t bufferSize);


    /* Callback used when to report the request line. */
    virtual void onRequestStart(const char * methodData, size_t methodSize,
                                const char * urlData, size_t urlSize,
                                const char * versionData,
                                size_t versionSize) = 0;

    /* Callback used to report a header-line, including the header key and the
     * value. */
    virtual void onHeader(const char * data, size_t dataSize) = 0;

    /* Callback used to report a chunk of the response body. Only invoked
       when the body is larger than 0 byte. */
    virtual void onData(const char * data, size_t dataSize) = 0;

    /* Callback used to report the end of a response. */
    virtual void onDone(bool requireClose) = 0;

private:
    HttpRequestParser parser_;
};


/****************************************************************************/
/* ASIO HTTP CLASSIC HANDLER                                                */
/****************************************************************************/

struct AsioHttpClassicHandler : public AsioHttpHandler {
    AsioHttpClassicHandler(boost::asio::ip::tcp::socket && socket);

    virtual void handleHttpPayload(const HttpHeader & header,
                                   const std::string & payload) = 0;

    void putResponseOnWire(const HttpResponse & response,
                           std::function<void ()> onSendFinished
                           = std::function<void ()>(),
                           PassiveConnectionHandler::NextAction next
                           = PassiveConnectionHandler::NEXT_CONTINUE);
    void send(const std::string & str,
              PassiveConnectionHandler::NextAction action
              = PassiveConnectionHandler::NEXT_CONTINUE,
              PassiveConnectionHandler::OnWriteFinished onWriteFinished
              = nullptr);

private:
    virtual void onRequestStart(const char * methodData, size_t methodSize,
                                const char * urlData, size_t urlSize,
                                const char * versionData,
                                size_t versionSize);
    virtual void onHeader(const char * data, size_t dataSize);
    virtual void onData(const char * data, size_t dataSize);
    virtual void onDone(bool requireClose);

    std::string headerPayload;
    std::string bodyPayload;
    bool bodyStarted_;

    std::string writeData_;
};

} // namespace Datacratic
