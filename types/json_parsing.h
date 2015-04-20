/* json_parsing.h                                                  -*- C++ -*-
   Jeremy Barnes, 1 February 2012
   Copyright (c) 2012 Datacratic Inc.  All rights reserved.

   Released under the MIT license.

   Functionality to ease parsing of JSON from within a parse_function.
*/

#pragma once

#include <string>
#include <functional>
#include "jml/utils/parse_context.h"
#include <boost/lexical_cast.hpp>


namespace Datacratic {

/*****************************************************************************/
/* JSON UTILITIES                                                            */
/*****************************************************************************/

std::string jsonEscape(const std::string & str);

void jsonEscape(const std::string & str, std::ostream & out);

std::string expectJsonString(ML::Parse_Context & context);

/*
 * Output goes into the given buffer, of the given maximum length.
 * If it doesn't fit, then return zero.
 */
ssize_t expectJsonString(ML::Parse_Context & context, char * buf,
                             size_t maxLength);

bool matchJsonString(ML::Parse_Context & context, std::string & str);

bool matchJsonNull(ML::Parse_Context & context);

void
expectJsonArray(ML::Parse_Context & context,
                const std::function<void (int, ML::Parse_Context &)> & onEntry);

void
expectJsonObject(ML::Parse_Context & context,
                 const std::function<void (const std::string &, ML::Parse_Context &)> & onEntry);

bool
matchJsonObject(ML::Parse_Context & context,
                const std::function<bool (const std::string &, ML::Parse_Context &)> & onEntry);

void skipJsonWhitespace(ML::Parse_Context & context);

inline bool expectJsonBool(ML::Parse_Context & context)
{
    if (context.match_literal("true"))
        return true;
    else if (context.match_literal("false"))
        return false;
    context.exception("expected bool (true or false)");
}

/** Representation of a numeric value in JSON.  It's designed to allow
    it to be stored the same way it was written (as an integer versus
    floating point, signed vs unsigned) without losing precision.
*/
struct JsonNumber {
    enum Type {
        NONE,
        UNSIGNED_INT,
        SIGNED_INT,
        FLOATING_POINT
    } type;

    union {
        unsigned long long uns;
        long long sgn;
        double fp;
    };    
};

/** Expect a JSON number.  This function is written in this strange way
    because JsonCPP is not a require dependency of jml, but the function
    needs to be out-of-line.
*/
JsonNumber expectJsonNumber(ML::Parse_Context & context);

/** Match a JSON number. */
bool matchJsonNumber(ML::Parse_Context & context, JsonNumber & num);

#ifdef CPPTL_JSON_H_INCLUDED

inline Json::Value
expectJson(ML::Parse_Context & context)
{
    context.skip_whitespace();
    if (*context == '"')
        return expectJsonString(context);
    else if (context.match_literal("null"))
        return Json::Value();
    else if (context.match_literal("true"))
        return Json::Value(true);
    else if (context.match_literal("false"))
        return Json::Value(false);
    else if (*context == '[') {
        Json::Value result(Json::arrayValue);
        expectJsonArray(context,
                        [&] (int i, ML::Parse_Context & context)
                        {
                            result[i] = expectJson(context);
                        });
        return result;
    } else if (*context == '{') {
        Json::Value result(Json::objectValue);
        expectJsonObject(context,
                         [&] (const std::string & key, ML::Parse_Context & context)
                         {
                             result[key] = expectJson(context);
                         });
        return result;
    } else {
        JsonNumber number = expectJsonNumber(context);
        switch (number.type) {
        case JsonNumber::UNSIGNED_INT:
            return number.uns;
        case JsonNumber::SIGNED_INT:
            return number.sgn;
        case JsonNumber::FLOATING_POINT:
            return number.fp;
        default:
            throw ML::Exception("logic error in expectJson");
        }
    }
}

#endif

} // namespace Datacratic

