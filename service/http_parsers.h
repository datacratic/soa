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
    void handleHeader(const char * data, size_t dataSize);
    void finalizeParsing();

    int state_;
    std::string buffer_;

    uint64_t remainingBody_;
    bool requireClose_;
};

}
