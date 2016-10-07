/* json_parsing.cc
   Jeremy Barnes, 8 March 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

*/

#include "jml/utils/json_parsing.h"

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

Utf8String
StreamingJsonParsingContext::
expectStringUtf8()
{
    skipJsonWhitespace((*context));
    context->expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;

    // Keep expanding until it fits
    while (!context->match_literal('"')) {
        // We need up to 4 characters to add a new UTF-8 code point
        if (pos >= bufferSize - 4) {
            size_t newBufferSize = bufferSize * 8;
            char * newBuffer = new char[newBufferSize];
            std::copy(buffer, buffer + bufferSize, newBuffer);
            if (buffer != internalBuffer)
                delete[] buffer;
            buffer = newBuffer;
            bufferSize = newBufferSize;
        }

        int c = *(*context);
        
        //cerr << "c = " << c << " " << (char)c << endl;

        if (c < 0 || c > 127) {
            // Unicode
            c = utf8::unchecked::next(*context);

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;

            continue;
        }
        ++(*context);

        if (c == '\\') {
            c = *(*context)++;
            switch (c) {
            case 't': c = '\t';  break;
            case 'n': c = '\n';  break;
            case 'r': c = '\r';  break;
            case 'f': c = '\f';  break;
            case 'b': c = '\b';  break;
            case '/': c = '/';   break;
            case '\\':c = '\\';  break;
            case '"': c = '"';   break;
            case 'u': {
                int code = context->expect_hex4();
                c = code;
                break;
            }
            default:
                context->exception("invalid escaped char");
            }
        }

        if (c < ' ' || c >= 127) {
            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(c, p2) - p1;
        }
        else buffer[pos++] = c;
    }

    Utf8String result(string(buffer, buffer + pos));
    if (buffer != internalBuffer)
        delete[] buffer;
    
    return result;
}

Json::Value
expectJson(ML::Parse_Context & context)
{
    context.skip_whitespace();
    if (*context == '"')
        return ML::expectJsonStringUTF8(context);
    else if (context.match_literal("null"))
        return Json::Value();
    else if (context.match_literal("true"))
       return Json::Value(true);
    else if (context.match_literal("false"))
        return Json::Value(false);
    else if (*context == '[') {
        Json::Value result(Json::arrayValue);
        ML::expectJsonArray(context,
                            [&] (int i, Parse_Context & context)
                            {
                                result[i] = expectJson(context);
                            });
        return result;
    } else if (*context == '{') {
        Json::Value result(Json::objectValue);
        ML::expectJsonObject(context,
                             [&] (const std::string & key, Parse_Context & context)
                             {
                                 result[key] = expectJson(context);
                             });
        return result;
   } else {
        JsonNumber number = ML::expectJsonNumber(context);
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

Json::Value
expectJsonAscii(ML::Parse_Context & context)
{
    context.skip_whitespace();
    if (*context == '"')
        return expectJsonStringAscii(context);
    else if (context.match_literal("null"))
        return Json::Value();
    else if (context.match_literal("true"))
        return Json::Value(true);
    else if (context.match_literal("false"))
        return Json::Value(false);
    else if (*context == '[') {
        Json::Value result(Json::arrayValue);
        ML::expectJsonArray(context,
                            [&] (int i, Parse_Context & context)
                            {
                                result[i] = expectJsonAscii(context);
                            });
        return result;
   } else if (*context == '{') {
        Json::Value result(Json::objectValue);
        ML::expectJsonObjectAscii(context,
                                  [&] (const char * key,
                                       ML::Parse_Context & context)
                                  {
                                      result[key] = expectJsonAscii(context);
                                  });
        return result;
    } else {
        JsonNumber number = ML::expectJsonNumber(context);
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

}  // namespace Datacratic
