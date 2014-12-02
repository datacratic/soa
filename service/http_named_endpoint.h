/* http_named_endpoint.h                                           -*- C++ -*-
   Jeremy Barnes, 9 November 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.
*/

#pragma once

#include "soa/service/http_endpoint.h"
#include "jml/utils/vector_utils.h"
#include "named_endpoint.h"
#include "http_rest_proxy.h"
#include "http_rest_endpoint.h"


namespace Datacratic {



/*****************************************************************************/
/* HTTP NAMED ENDPOINT                                                       */
/*****************************************************************************/

/** A message loop-compatible endpoint for http connections. */

struct HttpNamedEndpoint : public NamedEndpoint, public HttpRestEndpoint {

    HttpNamedEndpoint();

    void init(std::shared_ptr<ConfigurationService> config,
              const std::string & endpointName);

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
    std::string
    bindTcp(PortRange const & portRange, std::string host = "");

};


/*****************************************************************************/
/* HTTP NAMED REST PROXY                                                     */
/*****************************************************************************/

/** Proxy to connect to a named http-based service. */

struct HttpNamedRestProxy: public HttpRestProxy {

    void init(std::shared_ptr<ConfigurationService> config);

    bool connectToServiceClass(const std::string & serviceClass,
                               const std::string & endpointName,
                               bool local = true);

    bool connect(const std::string & endpointName);

    /** Called back when one of our endpoints either changes or disappears. */
    bool onConfigChange(ConfigurationService::ChangeType change,
                        const std::string & key,
                        const Json::Value & newValue);


private:
    std::shared_ptr<ConfigurationService> config;

    bool connected;
    std::string serviceClass;
    std::string endpointName;
};

} // namespace Datacratic

