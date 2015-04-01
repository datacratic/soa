/* rest_request_binding.cc                                         -*- C++ -*-
   Jeremy Barnes, 21 May 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "rest_request_binding.h"
#include "http_exception.h"

using namespace std;


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

std::function<bool
              (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)>
createRequestValidater(const Json::Value & argHelp)
{
    //cerr << "creating validator with help " << argHelp << endl;

    std::set<std::string> acceptedParams;

    if (!argHelp.isNull()) {
        for (auto & p: argHelp["requestParams"]) {
            string s = p["name"].asString();
            acceptedParams.insert(s);
        }
    }

    auto result = [=] (RestConnection & connection,
                       const RestRequest & request,
                       const RestRequestParsingContext & context)
        {
            bool hadError = false;
            Json::Value details;
            
            for (auto & s: request.params) {
                if (!acceptedParams.count(s.first)) {
                    hadError = true;
                    Json::Value detail;
                    detail["paramName"] = s.first;
                    detail["paramValue"] = s.second;

                    details["unknownParameters"].append(detail);
                }
            }

            if (!hadError)
                return true;  // pass the request
            
            details["help"] = argHelp;
            details["verb"] = request.verb;
            details["resource"] = request.resource;

            Json::Value exc;
            exc["error"] = "Unknown parameter(s) in REST call";
            exc["httpCode"] = 400;
            exc["details"] = details;

            connection.sendErrorResponse(400, exc);
            return false;
        };

    return result;
}



} // namespace Datacratic
