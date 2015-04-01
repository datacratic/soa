/** http_exception.h                                               -*- C++ -*-
    Jeremy Barnes, 13 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights.

    Exception class to use to return HTTP exceptions.
*/

#pragma once

#include "jml/arch/exception.h"
#include "soa/any/any.h"

namespace Datacratic {

struct HttpReturnException: public ML::Exception {
    HttpReturnException(int code, const std::string & message, Any body)
        : ML::Exception(message), code(code), body(body)
    {
    }

    HttpReturnException(int code, const std::string & message)
        : ML::Exception(message), code(code)
    {
    }

    ~HttpReturnException() throw ()
    {
    }

    int code;
    Any body;
};

} // namespace Datacratic
