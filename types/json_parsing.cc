/* json_parsing.cc
   Jeremy Barnes, 1 February 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Released under the MIT license.
*/

#include "json_parsing.h"
#include "jml/arch/format.h"
#include "soa/utf8cpp/source/utf8.h"


using namespace std;
using namespace ML;


namespace Datacratic {

/*****************************************************************************/
/* JSON UTILITIES                                                            */
/*****************************************************************************/


// Simple iterator wrapper around Parse_Context to enable the use of utf8cpp
// functions which require an iterator range.
struct ContextIterator : public std::iterator<std::input_iterator_tag, char>
{
    ContextIterator() : context(nullptr) {}
    ContextIterator(Parse_Context& context) : context(&context) {}

    char operator * () const { return **context; }

    ContextIterator& operator++() { (*context)++; return *this; }
    ContextIterator& operator++(int) { (*context)++; return *this; }

    bool operator!=(const ContextIterator& other) const
    {
        return !this->operator==(other);
    }

    bool operator==(const ContextIterator& other) const
    {
        auto eof = [&] (Parse_Context* context) {
            return !context || context->eof();
        };

        return eof(context) == eof(other.context);
    }

private:
    Parse_Context* context;
};

inline bool isStrictAscii(uint32_t codepoint) {
    return codepoint < 0x80;
}

void skipJsonWhitespace(Parse_Context & context)
{
    // Fast-path for the usual case for not EOF and no whitespace
    if (JML_LIKELY(!context.eof())) {
        char c = *context;
        if (c > ' ') {
            return;
        }
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r')
            return;
    }

    while (!context.eof()
           && (context.match_whitespace() || context.match_eol()));
}

char * jsonEscapeCore(const std::string & str, char * p, char * end)
{
    for (unsigned i = 0;  i < str.size();  ++i) {
        if (p + 4 >= end)
            return 0;

        char c = str[i];
        if (c >= ' ' && c < 127 && c != '\"' && c != '\\')
            *p++ = c;
        else {
            *p++ = '\\';
            switch (c) {
            case '\t': *p++ = ('t');  break;
            case '\n': *p++ = ('n');  break;
            case '\r': *p++ = ('r');  break;
            case '\f': *p++ = ('f');  break;
            case '\b': *p++ = ('b');  break;
            case '/':
            case '\\':
            case '\"': *p++ = (c);  break;
            default:
                throw Exception("Invalid character in JSON string: " + str);
            }
        }
    }

    return p;
}

std::string
jsonEscape(const std::string & str)
{
    size_t sz = str.size() * 4 + 4;
    char buf[sz];
    char * p = buf, * end = buf + sz;

    p = jsonEscapeCore(str, p, end);

    if (!p)
        throw ML::Exception("To fix: logic error in JSON escaping");

    return string(buf, p);
}

void jsonEscape(const std::string & str, std::ostream & stream)
{
    size_t sz = str.size() * 4 + 4;
    char buf[sz];
    char * p = buf, * end = buf + sz;

    p = jsonEscapeCore(str, p, end);

    if (!p)
        throw ML::Exception("To fix: logic error in JSON escaping");

    stream.write(buf, p - buf);
}

bool matchJsonString(Parse_Context & context, std::string & str)
{
    Parse_Context::Revert_Token token(context);

    skipJsonWhitespace(context);
    if (!context.match_literal('"')) return false;

    std::string result;

    while (!context.match_literal('"')) {
        if (context.eof()) return false;
        int codepoint = *context;
        if (!isStrictAscii(codepoint)) {
            ContextIterator it(context), end;
            codepoint = utf8::next(it, end);
            utf8::unchecked::append(codepoint, std::back_inserter(result));
            continue;
        }
        ++context;

        if (codepoint == '\\') {
            codepoint = *context++;
            switch (codepoint) {
            case 't': codepoint = '\t';  break;
            case 'n': codepoint = '\n';  break;
            case 'r': codepoint = '\r';  break;
            case 'f': codepoint = '\f';  break;
            case 'b': codepoint = '\b';  break;
            case '/': codepoint = '/';   break;
            case '\\':codepoint = '\\';  break;
            case '"': codepoint = '"';   break;
            case 'u': {
                codepoint = context.expect_hex4();
                break;
            }
            default:
                return false;
            }
        }
        if (!isStrictAscii(codepoint)) {
            utf8::append(codepoint, std::back_inserter(result));
        }
        else result.push_back(codepoint);
    }

    token.ignore();
    str = result;
    return true;
}

ssize_t expectJsonString(Parse_Context & context, char * buffer, size_t maxLength)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    size_t bufferSize = maxLength - 1;
    size_t pos = 0;

    // Try multiple times to make it fit
    while (!context.match_literal('"')) {
        int codepoint = *context;
        if (!isStrictAscii(codepoint)) {
            ContextIterator it(context), end;
            codepoint = utf8::next(it, end);

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::unchecked::append(codepoint, p2) - p1;

            continue;
        }

        ++context;
        if (codepoint == '\\') {
            codepoint = *context++;
            switch (codepoint) {
            case 't': codepoint = '\t';  break;
            case 'n': codepoint = '\n';  break;
            case 'r': codepoint = '\r';  break;
            case 'f': codepoint = '\f';  break;
            case 'b': codepoint = '\b';  break;
            case '/': codepoint = '/';   break;
            case '\\':codepoint = '\\';  break;
            case '"': codepoint = '"';   break;
            case 'u': {
                codepoint = context.expect_hex4();
                break;
            }
            default:
                context.exception("invalid escaped char");
            }
        }
        if (pos == bufferSize) {
            return -1;
        }

        if (!isStrictAscii(codepoint)) {
            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(codepoint, p2) - p1;
        }
        else buffer[pos++] = codepoint;
    }

    buffer[pos] = 0; // null terminator

    return pos;
}

std::string expectJsonString(Parse_Context & context)
{
    skipJsonWhitespace(context);
    context.expect_literal('"');

    char internalBuffer[4096];

    char * buffer = internalBuffer;
    size_t bufferSize = 4096;
    size_t pos = 0;

    // Try multiple times to make it fit
    while (!context.match_literal('"')) {

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

        int codepoint = *context;

        if (!isStrictAscii(codepoint)) {
            ContextIterator it(context), end;
            codepoint = utf8::next(it, end);

            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::unchecked::append(codepoint, p2) - p1;

            continue;
        }
        ++context;

        if (codepoint == '\\') {
            codepoint = *context++;
            switch (codepoint) {
            case 't': codepoint = '\t';  break;
            case 'n': codepoint = '\n';  break;
            case 'r': codepoint = '\r';  break;
            case 'f': codepoint = '\f';  break;
            case 'b': codepoint = '\b';  break;
            case '/': codepoint = '/';   break;
            case '\\':codepoint = '\\';  break;
            case '"': codepoint = '"';   break;
            case 'u': {
                codepoint = context.expect_hex4();
                break;
            }
            default:
                context.exception("invalid escaped char");
            }
        }
        if (!isStrictAscii(codepoint)) {
            char * p1 = buffer + pos;
            char * p2 = p1;
            pos += utf8::append(codepoint, p2) - p1;
        }
        else buffer[pos++] = codepoint;
    }
    
    string result(buffer, buffer + pos);
    if (buffer != internalBuffer)
        delete[] buffer;
    
    return result;
}

bool
matchJsonNull(Parse_Context & context)
{
    skipJsonWhitespace(context);
    return context.match_literal("null");
}

void
expectJsonArray(Parse_Context & context,
                const std::function<void (int, Parse_Context &)> & onEntry)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return;

    context.expect_literal('[');
    skipJsonWhitespace(context);
    if (context.match_literal(']')) return;

    for (int i = 0;  ; ++i) {
        skipJsonWhitespace(context);

        onEntry(i, context);

        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    context.expect_literal(']');
}

void
expectJsonObject(Parse_Context & context,
                 const std::function<void (const std::string &, Parse_Context &)> & onEntry)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return;

    context.expect_literal('{');

    skipJsonWhitespace(context);

    if (context.match_literal('}')) return;

    for (;;) {
        skipJsonWhitespace(context);

        string key = expectJsonString(context);

        skipJsonWhitespace(context);

        context.expect_literal(':');

        skipJsonWhitespace(context);

        onEntry(key, context);

        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    context.expect_literal('}');
}

bool
matchJsonObject(Parse_Context & context,
                const std::function<bool (const std::string &, Parse_Context &)> & onEntry)
{
    skipJsonWhitespace(context);

    if (context.match_literal("null"))
        return true;

    if (!context.match_literal('{')) return false;
    skipJsonWhitespace(context);
    if (context.match_literal('}')) return true;

    for (;;) {
        skipJsonWhitespace(context);

        string key = expectJsonString(context);

        skipJsonWhitespace(context);
        if (!context.match_literal(':')) return false;
        skipJsonWhitespace(context);

        if (!onEntry(key, context)) return false;

        skipJsonWhitespace(context);

        if (!context.match_literal(',')) break;
    }

    skipJsonWhitespace(context);
    if (!context.match_literal('}')) return false;

    return true;
}

JsonNumber expectJsonNumber(Parse_Context & context)
{
    JsonNumber result;

    std::string number;
    number.reserve(32);

    bool negative = false;
    bool doublePrecision = false;

    if (context.match_literal('-')) {
        number += '-';
        negative = true;
    }

    // EXTENSION: accept NaN and positive or negative infinity
    if (context.match_literal('N')) {
        context.expect_literal("aN");
        result.fp = negative ? -NAN : NAN;
        result.type = JsonNumber::FLOATING_POINT;
        return result;
    }
    else if (context.match_literal('n')) {
        context.expect_literal("an");
        result.fp = negative ? -NAN : NAN;
        result.type = JsonNumber::FLOATING_POINT;
        return result;
    }
    else if (context.match_literal('I') || context.match_literal('i')) {
        context.expect_literal("nf");
        result.fp = negative ? -INFINITY : INFINITY;
        result.type = JsonNumber::FLOATING_POINT;
        return result;
    }

    while (context && isdigit(*context)) {
        number += *context++;
    }

    if (context.match_literal('.')) {
        doublePrecision = true;
        number += '.';

        while (context && isdigit(*context)) {
            number += *context++;
        }
    }

    char sci = context ? *context : '\0';
    if (sci == 'e' || sci == 'E') {
        doublePrecision = true;
        number += *context++;

        char sign = context ? *context : '\0';
        if (sign == '+' || sign == '-') {
            number += *context++;
        }

        while (context && isdigit(*context)) {
            number += *context++;
        }
    }

    try {
        JML_TRACE_EXCEPTIONS(false);
        if (number.empty())
            context.exception("expected number");

        if (doublePrecision) {
            char * endptr = 0;
            errno = 0;
            result.fp = strtod(number.c_str(), &endptr);
            if (errno || endptr != number.c_str() + number.length())
                context.exception(ML::format("failed to convert '%s' to long long",
                                             number.c_str()));
            result.type = JsonNumber::FLOATING_POINT;
        } else if (negative) {
            char * endptr = 0;
            errno = 0;
            result.sgn = strtol(number.c_str(), &endptr, 10);
            if (errno || endptr != number.c_str() + number.length())
                context.exception(ML::format("failed to convert '%s' to long long",
                                             number.c_str()));
            result.type = JsonNumber::SIGNED_INT;
        } else {
            char * endptr = 0;
            errno = 0;
            result.uns = strtoull(number.c_str(), &endptr, 10);
            if (errno || endptr != number.c_str() + number.length())
                context.exception(ML::format("failed to convert '%s' to unsigned long long",
                                             number.c_str()));
            result.type = JsonNumber::UNSIGNED_INT;
        }
    } catch (const std::exception & exc) {
        context.exception("expected number");
    }

    return result;
}

} // namespace Datacratic
