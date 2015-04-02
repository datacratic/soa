/** http_exception.h                                               -*- C++ -*-
    Jeremy Barnes, 13 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights.

    Exception class to use to return HTTP exceptions.
*/

#pragma once

#include "jml/arch/exception.h"
#include "soa/any/any.h"
#include "soa/types/string.h"

namespace Datacratic {

struct HttpReturnException: public ML::Exception {
    HttpReturnException(int code, const Utf8String & message, Any body = Any())
        : ML::Exception(message.rawData()), code(code), body(body)
    {
    }

    HttpReturnException(int code, const std::string & message, Any body = Any())
        : ML::Exception(message), code(code), body(body)
    {
    }

    ~HttpReturnException() throw ()
    {
    }

    int code;
    Any body;
};


/** Rethrow an exception, adding some extra context to it.  The exception is
    obtained from std::current_exception().
*/
void rethrowHttpException(int code, const Utf8String & message, Any details = Any()) JML_NORETURN;
void rethrowHttpException(int code, const std::string & message, Any details = Any()) JML_NORETURN;

} // namespace Datacratic
