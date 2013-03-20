/* remote_input.h                                                  -*- C++ -*-
   Jeremy Barnes, 26 May 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   The input side of a remote logging connection.
*/

#pragma once

#include "logger.h"
#include "soa/service/passive_endpoint.h"


namespace Datacratic {

struct RemoteInput {
    
    RemoteInput();

    ~RemoteInput();

    /** Listen on the given port. */
    void listen(int port,
                const std::string & address,
                std::function<void ()> onShutdown = 0);

    /** Shutdown and stop listening. */
    void shutdown();

    /** What port are we listening on? */
    int port() const
    {
        return endpoint.port();
    }

    /** Function used to respond to having data. */
    std::function<void (const std::string &)> onData;

private:
    PassiveEndpointT<SocketTransport> endpoint;
    std::function<void ()> onShutdown;
};

} // namespace Datacratic
