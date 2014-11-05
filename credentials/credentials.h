/* credentials.h                                                   -*- C++ -*-
   Jeremy Barnes, 5 November 2014
   
   A pluggable mechanism for getting credentials.
*/

#pragma once

#include "soa/types/value_description.h"

namespace Datacratic {

struct Credential {
    std::string id;
    std::string secret;

    Json::Value metadata;
};

DECLARE_STRUCTURE_DESCRIPTION(Credential);

struct CredentialContext {
    std::string user;
    Json::Value userid;
};

DECLARE_STRUCTURE_DESCRIPTION(CredentialContext);

/** Return credentials for the given resource of the given resource type.

    If none are available, then returns an empty list.
*/
std::vector<Credential>
getCredentials(const std::string & resourceType,
               const std::string & resource,
               const CredentialContext & context,
               Json::Value extraData);

} // namespace Datacratic
