/** rest_request_params.cc
    Jeremy Barnes, 18 April 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.
*/


#include "rest_request_params.h"


namespace Datacratic {


const std::string & restEncode(const Utf8String & str)
{
    return str.rawString();
}

Utf8String restDecode(std::string str, Utf8String *)
{
    return Utf8String(std::move(str));
}

bool restDecode(const std::string & str, bool *)
{
    if (str == "true")
        return true;
    else if (str == "false")
        return false;
    else return boost::lexical_cast<bool>(str);
}

std::string restEncode(bool b)
{
    return std::to_string(b);
}

} // namespace Datacratic
