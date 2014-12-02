/* json_service_endpoint.h                                         -*- C++ -*-
   Jeremy Barnes, 9 November 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

*/

#pragma once

#include "http_header.h"

namespace Datacratic {

/*****************************************************************************/
/* REST REQUEST                                                              */
/*****************************************************************************/

struct RestRequest {
    RestRequest()
    {
    }

    RestRequest(const HttpHeader & header,
                const std::string & payload)
        : header(header),
          verb(header.verb),
          resource(header.resource),
          params(header.queryParams),
          payload(payload)
    {
    }

    RestRequest(const std::string & verb,
                const std::string & resource,
                const RestParams & params,
                const std::string & payload)
        : verb(verb), resource(resource), params(params), payload(payload)
    {
    }

    HttpHeader header;
    std::string verb;
    std::string resource;
    RestParams params;
    std::string payload;
};

std::ostream & operator << (std::ostream & stream, const RestRequest & request);

} // namespace Datacratic

