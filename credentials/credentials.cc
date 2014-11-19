/** credentials.cc
    Jeremy Barnes, 5 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "credentials.h"
#include "soa/types/basic_value_descriptions.h"

using namespace std;

namespace Datacratic {

DEFINE_STRUCTURE_DESCRIPTION(Credential);

CredentialDescription::
CredentialDescription()
{
    addField("provider", &Credential::provider,
             "Provider of credentials");
    addField("protocol", &Credential::protocol,
             "Protocol to use to access the service");
    addField("location", &Credential::location,
             "Location of the service");
    addField("id", &Credential::id,
             "User ID to use to access the service");
    addField("secret", &Credential::secret,
             "Secret key to use to access the service");
    addField("extra", &Credential::extra,
             "Extra configuration needed to access the service");
    addField("validUntil", &Credential::validUntil,
             "Time until which the credential is valid");
}

DEFINE_STRUCTURE_DESCRIPTION(CredentialContext);

CredentialContextDescription::
CredentialContextDescription()
{
}

} // namespace Datacratic
