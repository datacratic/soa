/** credentials.cc
    Jeremy Barnes, 5 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "credentials.h"

using namespace std;

namespace Datacratic {

DEFINE_STRUCTURE_DESCRIPTION(Credential);

CredentialDescription::
CredentialDescription()
{
}

DEFINE_STRUCTURE_DESCRIPTION(CredentialContext);

CredentialContextDescription::
CredentialContextDescription()
{
}

std::vector<Credential>
getCredentials(const std::string & resourceType,
               const std::string & resource,
               const CredentialContext & context,
               Json::Value extraData)
{
    return {};
}


} // namespace Datacratic
