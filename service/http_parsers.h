/* http_parsers.h                                                  -*- C++ -*-
   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

*/

#pragma once

#include <functional>


namespace Datacratic {

/****************************************************************************/
/* HTTP RESPONSE PARSER                                                     */
/****************************************************************************/

struct HttpResponseParser {
    typedef std::function<void (const std::string &, int)> OnResponseStart;
    typedef std::function<void (const char *, size_t)> OnData;
    typedef std::function<void (bool)> OnDone;

    HttpResponseParser()
        noexcept
    {
        clear();
    }

    void clear() noexcept;

    void feed(const char * data);
    void feed(const char * data, size_t size);

    uint64_t remainingBody() const
    {
        return remainingBody_;
    }

    OnResponseStart onResponseStart;
    OnData onHeader;
    OnData onData;
    OnDone onDone;

private:
    /* structure to hold the temporary state of the parser used when "feed" is
       invoked */
    struct BufferState {
        BufferState()
            : data(nullptr), dataSize(0), fromBuffer(false),
              ptr(0), commited(0)
        {
        }

        /* skip as many characters as possible until "c" is found */
        bool skipToChar(char c, bool throwOnEol);

        /* number of bytes available for parsing in the buffer */
        size_t remaining() const { return dataSize - ptr; }

        /* number of uncommited bytes available for parsing in the buffer */
        size_t remainingUncommited() const { return dataSize - commited; }

        /* pointer to the current byte ptr */
        const char * currentDataPtr() const { return data + ptr; }

        /* commit the value of ptr */
        void commit()
        {
            commited = ptr;
        }

        const char * data;
        size_t dataSize;
        bool fromBuffer;
        size_t ptr;
        size_t commited;
    };

    BufferState prepareParsing(const char * bufferData, size_t bufferSize);
    bool parseStatusLine(BufferState & state);
    bool parseHeaders(BufferState & state);
    bool parseBody(BufferState & state);
    bool parseChunkedBody(BufferState & state);
    bool parseBlockBody(BufferState & state);

    void handleHeader(const char * data, size_t dataSize);
    void finalizeParsing();

    int stage_;
    std::string buffer_;

    uint64_t remainingBody_;
    bool useChunkedEncoding_;
    bool requireClose_;
};

}
