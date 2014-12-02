/* http_rest_endpoint.h                                            -*- C++ -*-
   Jeremy Barnes, 9 November 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.
*/

#pragma once

#include "soa/service/http_endpoint.h"
#include <atomic>

namespace Datacratic {


/*****************************************************************************/
/* HTTP REST ENDPOINT                                                        */
/*****************************************************************************/

/** An HTTP endpoint for REST calls over HTTP connections. */

struct HttpRestEndpoint: public HttpEndpoint {

    HttpRestEndpoint();

    /** Set the Access-Control-Allow-Origin: * HTTP header */
    void allowAllOrigins();

    void init();

    /** Bid into a given address.  Address is host:port.

        If no port is given (and no colon), than use any port number.
        If port is a number and then "+", then scan for any port higher than
        the given number.
        If host is empty or "*", then use all interfaces.
    */
    std::string
    bindTcpAddress(const std::string & address);

    /** Bind into a specific tcp port.  If the port is not available, it will
        throw an exception.

        Returns the uri to connect to.
    */
    std::string
    bindTcpFixed(std::string host, int port);

    /** Bind into a tcp port.  If the preferred port is not available, it will
        scan until it finds one that is.

        Returns the uri to connect to.
    */
    virtual std::string
    bindTcp(PortRange const & portRange, std::string host = "");
    
    /** Connection handler structure for the endpoint. */
    struct RestConnectionHandler: public HttpConnectionHandler {
        RestConnectionHandler(HttpRestEndpoint * endpoint);

        HttpRestEndpoint * endpoint;
        std::weak_ptr<RestConnectionHandler> sharedThis;

        /** Disconnect handler. */
        std::function<void ()> onDisconnect;

        virtual void
        handleHttpPayload(const HttpHeader & header,
                          const std::string & payload);

        /** Called when the other end disconnects from us.  We set the
            zombie flag and stop anything else from happening on the
            socket once we're done.
        */
        virtual void handleDisconnect();

        void sendErrorResponse(int code, const std::string & error);

        void sendErrorResponse(int code, const Json::Value & error);

        void sendResponse(int code,
                          const Json::Value & response,
                          const std::string & contentType = "application/json",
                          RestParams headers = RestParams());

        void sendResponse(int code,
                          const std::string & body,
                          const std::string & contentType,
                          RestParams headers = RestParams());

        void sendResponseHeader(int code,
                                const std::string & contentType,
                                RestParams headers = RestParams());

        /** Send an HTTP chunk with the appropriate headers back down the
            wire. */
        void sendHttpChunk(const std::string & chunk,
                           NextAction next = NEXT_CONTINUE,
                           OnWriteFinished onWriteFinished = OnWriteFinished());

        /** Send the entire HTTP payload.  Its length must match that of
            the response header.
        */
        void sendHttpPayload(const std::string & str);

        mutable std::mutex mutex;

    public:
        /// If this is true, the connection has no transport
        std::atomic<bool> isZombie;
    };

    typedef std::function<void (std::shared_ptr<RestConnectionHandler> connection,
                                const HttpHeader & header,
                                const std::string & payload)> OnRequest;

    OnRequest onRequest;

    std::vector<std::pair<std::string, std::string> > extraHeaders;

    virtual std::shared_ptr<ConnectionHandler>
    makeNewHandler();
};

} // namespace Datacratic
