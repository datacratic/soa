/* rest_request_binding.cc                                         -*- C++ -*-
   Jeremy Barnes, 21 May 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "rest_request_binding.h"

namespace Datacratic {

/** These functions turn an argument to the request binding into a function
    that can generate the value required by the handler function.

*/

std::function<std::string
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const StringPayload & p, void *)
{
    Json::Value & v = argHelp["payload"];
    v["description"] = p.description;

    return [=] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        {
            return request.payload;
        };
}

/** Pass the connection on */
std::function<RestConnection &
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassConnectionId &, void *)
{
    return [] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        -> RestConnection &
        {
            return connection;
        };
}

/** Pass the connection on */
std::function<const RestRequestParsingContext &
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassParsingContext &, void *)
{
    return [] (RestConnection & connection,
                const RestRequest & request,
                const RestRequestParsingContext & context)
        -> const RestRequestParsingContext &
        {
            return context;
        };
}

/** Pass the connection on */
std::function<const RestRequest &
                     (RestConnection & connection,
                      const RestRequest & request,
                      const RestRequestParsingContext & context)>
createParameterExtractor(Json::Value & argHelp,
                         const PassRequest &, void *)
{
    return [] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
        -> const RestRequest &
        {
            return request;
        };
}



} // namespace Datacratic
