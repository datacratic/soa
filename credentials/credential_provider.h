/* credential_provider.h                                           -*- C++ -*-
   Jeremy Barnes, 5 November 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.
   
   Credential provider structure and registration.
*/

#include "soa/credentials/credentials.h"

#pragma once

namespace Datacratic {

struct CredentialProvider {
    virtual std::vector<Credential>
    getSync(const std::string & resourceType,
            const std::string & resource,
            const CredentialContext & context,
            Json::Value extraData) const = 0;

    static void registerProvider(const std::string & name,
                                 std::shared_ptr<CredentialProvider> provider);
};


} // namespace Datacratic
