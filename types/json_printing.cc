/* json_printing.cc
   Jeremy Barnes, 8 March 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

   Functionality to print JSON values.
*/

#include "jml/utils/exc_assert.h"

#include "json_printing.h"
#include "soa/utf8cpp/source/utf8.h"


using namespace std;


namespace Datacratic {


void
StreamJsonPrintingContext::
writeStringUtf8(const std::string & s)
{
    stream << '\"';

    utf8::iterator<std::string::const_iterator> it(s.begin(), s.begin(), s.end());
    utf8::iterator<std::string::const_iterator> end(s.end(), s.begin(), s.end());
    
    for (;  it != end;  ++it) {
        auto codepoint = *it;
        if (codepoint >= ' ' && codepoint < 127 && codepoint != '\"' && codepoint != '\\')
            stream << static_cast<char>(codepoint);
        else {
            switch (codepoint) {
            case '\t': stream << "\\t";  break;
            case '\n': stream << "\\n";  break;
            case '\r': stream << "\\r";  break;
            case '\b': stream << "\\b";  break;
            case '\f': stream << "\\f";  break;
            case '/':
            case '\\':
            case '\"': stream << '\\' << static_cast<char>(codepoint);  break;
            default:
                if (writeUtf8) {
                    char buf[4];
                    std::fill(buf, buf + sizeof buf, 0);
                    // utf8::iterator should already check for codepoint validity, so we
                    // don't need to check a second time when encoding the codepoint in the
                    // resulting buffer
                    char * p = utf8::unchecked::append(codepoint, buf);
                    stream.write(buf, p - buf);
                }
                else {
                    ExcAssert(codepoint >= 0 && codepoint < 65536);
                    stream << ML::format("\\u%04x", static_cast<unsigned>(codepoint));
                }
            }
        }
    }
    
    stream << '\"';
}


} // namespace Datacratic
