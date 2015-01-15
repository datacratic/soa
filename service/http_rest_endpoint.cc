/* http_rest_endpoint.cc
   Jeremy Barnes, 11 November 2012
   Copyright (c) 2012 Datacratic Inc.  All rights reserved.

   Named endpoint for http connections.
*/

#include "http_rest_endpoint.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/filter_streams.h"


using namespace std;


namespace Datacratic {


/*****************************************************************************/
/* HTTP REST ENDPOINT                                                        */
/*****************************************************************************/

HttpRestEndpoint::
HttpRestEndpoint()
    : HttpEndpoint("HttpRestEndpoint")
{
}

void
HttpRestEndpoint::
allowAllOrigins()
{
    extraHeaders.push_back({ "Access-Control-Allow-Origin", "*" });
}

void
HttpRestEndpoint::
init()
{
}

std::string
HttpRestEndpoint::
bindTcpAddress(const std::string & address)
{
    using namespace std;
    auto pos = address.find(':');
    if (pos == string::npos) {
        // No port specification; take any port
        return bindTcp(PortRange(12000, 12999), address);
    }
    string hostPart(address, 0, pos);
    string portPart(address, pos + 1);

    if (portPart.empty())
        throw ML::Exception("invalid port " + portPart + " in address "
                            + address);

    if (portPart[portPart.size() - 1] == '+') {
        unsigned port = boost::lexical_cast<unsigned>(string(portPart, 0, portPart.size() - 1));
        if(port < 65536) {
            unsigned last = port + 999;
            return bindTcp(PortRange(port, last), hostPart);
        }

        throw ML::Exception("invalid port " + port);
    }

    return bindTcp(boost::lexical_cast<int>(portPart), hostPart);
}

std::string
HttpRestEndpoint::
bindTcpFixed(std::string host, int port)
{
    return bindTcp(port, host);
}

std::string
HttpRestEndpoint::
bindTcp(PortRange const & portRange, std::string host)
{
    // TODO: generalize this...
    if (host == "" || host == "*")
        host = "0.0.0.0";

    // TODO: really scan ports
    int port = HttpEndpoint::listen(portRange, host, false /* name lookup */);
    const char * literate_doc_bind_file = getenv("LITERATE_DOC_BIND_FILENAME");
    if (literate_doc_bind_file) {
        Json::Value v;
        v["port"] = port;
        ML::filter_ostream out(literate_doc_bind_file);
        out << v.toString() << endl;
        out.close();
    }

    return "http://" + host + ":" + to_string(port);
}

std::shared_ptr<ConnectionHandler>
HttpRestEndpoint::
makeNewHandler()
{
    auto res = std::make_shared<RestConnectionHandler>(this);

    // Allow it to get a shared pointer to itself
    res->sharedThis = res;
    return res;
}


/*****************************************************************************/
/* HTTP NAMED ENDPOINT REST CONNECTION HANDLER                               */
/*****************************************************************************/

HttpRestEndpoint::RestConnectionHandler::
RestConnectionHandler(HttpRestEndpoint * endpoint)
    : endpoint(endpoint), isZombie(false)
{
}

void
HttpRestEndpoint::RestConnectionHandler::
handleHttpPayload(const HttpHeader & header,
                  const std::string & payload)
{
    // We don't lock here, since sending the response will take the lock,
    // and whatever called us must know it's a valid connection

    try {
        auto th = sharedThis.lock();
        ExcAssert(th);
        endpoint->onRequest(th, header, payload);
    }
    catch(const std::exception& ex) {
        Json::Value response;
        response["error"] =
            "exception processing request "
            + header.verb + " " + header.resource;

        response["exception"] = ex.what();
        sendErrorResponse(400, response);
    }
    catch(...) {
        Json::Value response;
        response["error"] =
            "exception processing request "
            + header.verb + " " + header.resource;

        sendErrorResponse(400, response);
    }
}

void
HttpRestEndpoint::RestConnectionHandler::
handleDisconnect()
{
    std::unique_lock<std::mutex> guard(mutex);
    //cerr << "*** Got handle disconnect for rest connection handler" << endl;
    isZombie = true;
    HttpConnectionHandler::handleDisconnect();
}

void
HttpRestEndpoint::RestConnectionHandler::
sendErrorResponse(int code, const std::string & error)
{
    if (isZombie)
        return;

    Json::Value val;
    val["error"] = error;

    sendErrorResponse(code, val);
}

void
HttpRestEndpoint::RestConnectionHandler::
sendErrorResponse(int code, const Json::Value & error)
{
    if (isZombie)
        return;

    std::string encodedError = error.toString();

    std::unique_lock<std::mutex> guard(mutex);
    if (isZombie)
        return;
    putResponseOnWire(HttpResponse(code, "application/json",
                                   encodedError, endpoint->extraHeaders),
                      nullptr, NEXT_CLOSE);
}

void
HttpRestEndpoint::RestConnectionHandler::
sendResponse(int code,
             const Json::Value & response,
             const std::string & contentType,
             RestParams headers)
{
    std::string body = response.toStyledString();
    return sendResponse(code, body, contentType, std::move(headers));
}

        

void
HttpRestEndpoint::RestConnectionHandler::
sendResponse(int code,
             const std::string & body,
             const std::string & contentType,
             RestParams headers)
{
    // Recycle back to a new handler once done so that the next connection can be
    // handled.
    auto onSendFinished = [=] {
        this->transport().associateWhenHandlerFinished
        (endpoint->makeNewHandler(), "sendResponse");
    };
    
    for (auto & h: endpoint->extraHeaders)
        headers.push_back(h);

    std::unique_lock<std::mutex> guard(mutex);
    if (isZombie)
        return;
    putResponseOnWire(HttpResponse(code, contentType, body, headers),
                      onSendFinished);
}

void
HttpRestEndpoint::RestConnectionHandler::
sendResponseHeader(int code,
                   const std::string & contentType,
                   RestParams headers)
{
    auto onSendFinished = [=] {
        // Do nothing once we've finished sending the response, so that
        // the connection isn't closed
    };
    
    for (auto & h: endpoint->extraHeaders)
        headers.push_back(h);

    std::unique_lock<std::mutex> guard(mutex);
    if (isZombie)
        return;
    putResponseOnWire(HttpResponse(code, contentType, headers),
                      onSendFinished);
}

void
HttpRestEndpoint::RestConnectionHandler::
sendHttpChunk(const std::string & chunk,
              NextAction next,
              OnWriteFinished onWriteFinished)
{
    std::unique_lock<std::mutex> guard(mutex);
    if (isZombie)
        return;
    HttpConnectionHandler::sendHttpChunk(chunk, next, onWriteFinished);
}


void
HttpRestEndpoint::RestConnectionHandler::
sendHttpPayload(const std::string & str)
{
    std::unique_lock<std::mutex> guard(mutex);
    if (isZombie)
        return;
    send(str);
}

} // namespace Datacratic

