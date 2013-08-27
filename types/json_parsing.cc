/* json_parsing.cc
   Jeremy Barnes, 8 March 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

*/

#include "json_parsing.h"
#include "string.h"
#include "jml/arch/format.h"

using namespace std;
using namespace ML;

namespace Datacratic {

inline uint8_t fromHex(char hex, Parse_Context & context)
{
    if (hex >= '0' && hex <= '9')
        return hex - '0';
    else if (hex >= 'a' && hex <= 'f')
        return hex - 'a' + 10;
    else if (hex >= 'A' && hex <= 'F')
        return hex - 'A' + 10;
    else
        context.exception(format("invalid hexadecimal: %c", hex));
}

inline uint16_t codepointFromHex(Parse_Context & context)
{
    uint16_t code = 0;
    for (int i = 0; i < 4; ++i)
        code = (code << 4) | fromHex(*context++, context);
    return code;
}

inline uint16_t expectJsonStringChar(Parse_Context & context, bool & isUnicode)
{
    isUnicode = false;
    char c = *context++;
    if (c != '\\')
        return c;

    c = *context++;
    switch (c) {
        case 't': return '\t';
        case 'n': return '\n';
        case 'r': return '\r';
        case 'f': return '\f';
        case 'b': return '\b';
        case '/': return '/';
        case '\\':return '\\';
        case '"': return '"';
        case 'u': isUnicode = true; return codepointFromHex(context);
        default: context.exception(format("invalid escape sequence: \\%c", c));
    }
}

string
StreamingJsonParsingContext::
expectString()
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

        bool isUnicode;
        uint16_t c = Datacratic::expectJsonStringChar(*context, isUnicode);

        if (isUnicode)
            pos = utf8::append(c, buffer + pos) - buffer;
        else
            buffer[pos++] = c;
    }

    if (buffer != internalBuffer)
        delete[] buffer;
    
    return string(buffer, buffer + pos);
}


}  // namespace Datacratic
