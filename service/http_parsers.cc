/* http_parsers.h                                                  -*- C++ -*-
   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

*/

#include <string.h>

#include "jml/arch/exception.h"
#include "jml/utils/string_functions.h"

#include "http_parsers.h"

using namespace std;
using namespace Datacratic;

/****************************************************************************/
/* HTTP RESPONSE PARSER                                                     */
/****************************************************************************/

void
HttpResponseParser::
clear()
    noexcept
{
    state_ = 0;
    buffer_.clear();
    remainingBody_ = 0;
    requireClose_ = false;
}

void
HttpResponseParser::
feed(const char * bufferData)
{
    // cerr << "feed: /" + ML::hexify_string(string(bufferData)) + "/\n";
    feed(bufferData, strlen(bufferData));
}

void
HttpResponseParser::
feed(const char * bufferData, size_t bufferSize)
{
    const char * data;
    size_t dataSize;
    bool fromBuffer;

    // cerr << ("data: /"
    //          + ML::hexify_string(string(bufferData, bufferSize))
    //          + "/\n");

    if (buffer_.size() > 0) {
        buffer_.append(bufferData, bufferSize);
        data = buffer_.c_str();
        fromBuffer = true;
        dataSize = buffer_.size();
    }
    else {
        data = bufferData;
        fromBuffer = false;
        dataSize = bufferSize;
    }

    size_t ptr = 0;

    auto skipToChar = [&] (char c, bool throwOnEol) {
        while (ptr < dataSize) {
            if (data[ptr] == c)
                return true;
            else if (throwOnEol
                     && (data[ptr] == '\r' || data[ptr] == '\n')) {
                throw ML::Exception("unexpected end of line");
            }
            ptr++;
        }

        return false;
    };

    // cerr << ("state: " + to_string(state_)
    //          + "; dataSize: " + to_string(dataSize) + "\n");

    while (true) {
        if (ptr == dataSize) {
            if (fromBuffer) {
                buffer_.clear();
            }
            return;
        }
        if (state_ == 0) {
            // status line
            // HTTP/1.1 200 OK

            /* sizeof("HTTP/1.1 200 ") */
            if ((dataSize - ptr) < 16) {
                if (!fromBuffer) {
                    buffer_.assign(data + ptr, dataSize - ptr);
                }
                return;
            }

            if (::memcmp(data + ptr, "HTTP/", 5) != 0) {
                throw ML::Exception("version must start with 'HTTP/'");
            }
            ptr += 5;

            if (!skipToChar(' ', true)) {
                /* post-version ' ' not found even though size is sufficient */
                throw ML::Exception("version too long");
            }
            size_t versionEnd = ptr;

            ptr++;
            size_t codeStart = ptr;
            if (!skipToChar(' ', true)) {
                /* post-code ' ' not found even though size is sufficient */
                throw ML::Exception("code too long");
            }

            size_t codeEnd = ptr;
            int code = ML::antoi(data + codeStart, data + codeEnd);

            /* we skip the whole "reason" string */
            if (!skipToChar('\r', false)) {
                if (!fromBuffer) {
                    buffer_.assign(data + ptr, dataSize - ptr);
                }
                return;
            }
            ptr++;
            if (ptr == dataSize) {
                return;
            }
            if (data[ptr] != '\n') {
                throw ML::Exception("expected \\n");
            }
            ptr++;
            onResponseStart(string(data, versionEnd), code);
            state_ = 1;

            if (ptr == dataSize) {
                buffer_.clear();
                return;
            }
        }
        else if (state_ == 1) {
            while (data[ptr] != '\r') {
                size_t headerPtr = ptr;
                if (!skipToChar(':', true) || !skipToChar('\r', false)) {
                    if (headerPtr > 0 || !fromBuffer) {
                        buffer_.assign(data + headerPtr,
                                       dataSize - headerPtr);
                    }
                    return;
                }
                ptr++;
                if (ptr == dataSize) {
                    if (headerPtr > 0 || !fromBuffer) {
                        buffer_.assign(data + headerPtr,
                                       dataSize - headerPtr);
                    }
                    return;
                }
                if (data[ptr] != '\n') {
                    throw ML::Exception("expected \\n");
                }
                ptr++;
                handleHeader(data + headerPtr, ptr - headerPtr - 2);
                if (ptr == dataSize) {
                    // cerr << "returning\n";
                    if (fromBuffer) {
                        buffer_.clear();
                    }
                    return;
                }
            }
            if (ptr + 1 == dataSize) {
                buffer_.assign(data + ptr, dataSize - ptr);
                return;
            }
            ptr++;
            if (data[ptr] != '\n') {
                throw ML::Exception("expected \\n");
            }
            ptr++;

            if (remainingBody_ == 0) {
                finalizeParsing();
            }
            else {
                state_ = 2;
            }

            if (dataSize == 0) {
                if (fromBuffer) {
                    buffer_.clear();
                }
                return;
            }
        }
        else if (state_ == 2) {
            uint64_t chunkSize = min(dataSize - ptr, remainingBody_);
            // cerr << "toSend: " + to_string(chunkSize) + "\n";
            // cerr << "received body: /" + string(data, chunkSize) + "/\n";
            onData(data + ptr, chunkSize);
            ptr += chunkSize;
            remainingBody_ -= chunkSize;
            if (remainingBody_ == 0) {
                finalizeParsing();
            }
        }
    }
}

void
HttpResponseParser::
handleHeader(const char * data, size_t dataSize)
{
    size_t ptr(0);

    auto skipToChar = [&] (char c) {
        while (ptr < dataSize) {
            if (data[ptr] == c)
                return true;
            ptr++;
        }

        return false;
    };
    auto skipChar = [&] (char c) {
        while (ptr < dataSize && data[ptr] == c) {
            ptr++;
        }
    };
    auto matchString = [&] (const char * testString, size_t len) {
        bool result;
        if (dataSize >= (ptr + len)
            && ::strncasecmp(data + ptr, testString, len) == 0) {
            ptr += len;
            result = true;
        }
        else {
            result = false;
        }
        return result;
    };

    if (matchString("Connection", 10)) {
        skipChar(' ');
        skipToChar(':');
        ptr++;
        skipChar(' ');
        if (matchString("close", 5)) {
            requireClose_ = true;
        }
    }
    else if (matchString("Content-Length", 14)) {
        skipChar(' ');
        skipToChar(':');
        ptr++;
        skipChar(' ');
        remainingBody_ = ML::antoi(data + ptr, data + dataSize);
    }

    onHeader(data, dataSize);
}

void
HttpResponseParser::
finalizeParsing()
{
    onDone(requireClose_);
    clear();
}
