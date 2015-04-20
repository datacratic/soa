/* json_parsing.cc
   Jeremy Barnes, 8 March 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

*/

#include "soa/jsoncpp/json.h"
#include "json_parsing.h"
#include "string.h"
#include "value_description.h"

using namespace std;
using namespace ML;

namespace Datacratic {

void
JsonParsingContext::
onUnknownField(const ValueDescription * desc)
{
    if (!onUnknownFieldHandlers.empty())
        onUnknownFieldHandlers.back()(desc);
    else {
        std::string typeNameStr = desc ? "parsing " + desc->typeName + " ": "";
        exception("unknown field " + typeNameStr + printPath());
    }
}

}  // namespace Datacratic
