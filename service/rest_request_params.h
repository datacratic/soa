/** rest_request_params.h                                          -*- C++ -*-
    Jeremy Barnes, 24 January 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include <boost/lexical_cast.hpp>
#include "json_codec.h"
#include "soa/types/value_description.h"

namespace Datacratic {

/*****************************************************************************/
/* REST CODEC                                                                */
/*****************************************************************************/

/** Default parameter encoder / decoder that uses a lexical cast to convert
    to the required type.
*/
template<typename T>
decltype(boost::lexical_cast<T>(std::declval<std::string>()))
restDecode(const std::string & str, T * = 0)
{
    return boost::lexical_cast<T>(str);
}

template<typename T>
std::string restEncode(const T & val,
                       decltype(boost::lexical_cast<std::string>(std::declval<T>())) * = 0)
{
    return boost::lexical_cast<std::string>(val);
}

const std::string & restEncode(const Utf8String & str);
Utf8String restDecode(std::string str, Utf8String *);
bool restDecode(const std::string & str, bool *);
std::string restEncode(bool b);

template<typename T, typename Enable = void>
struct RestCodec {
    static T decode(const std::string & str)
    {
        return restDecode(str, (T *)0);
    }

    static std::string encode(const T & val)
    {
        return restEncode(val);
    }
};

/*****************************************************************************/
/* JSON STR CODEC                                                            */
/*****************************************************************************/

/** Parameter encoder / decoder that takes parameters encoded in JSON as a
    string and converts them into the required type.
*/

template<typename T>
struct JsonStrCodec {
    JsonStrCodec(std::shared_ptr<const ValueDescriptionT<T> > desc
                 = getDefaultDescriptionShared<T>())
        : desc(std::move(desc))
    {
    }

    T decode(const std::string & str) const
    {
        T result;

        StreamingJsonParsingContext context(str, str.c_str(), str.length());
        desc->parseJson(&result, context);
        return result;
    }

    std::string encode(const T & obj) const
    {
        std::ostringstream stream;
        StreamJsonPrintingContext context(stream);
        desc->printJson(&obj, context);
        return std::move(stream.str());
    }

    std::shared_ptr<const ValueDescriptionT<T> > desc;
};


/*****************************************************************************/
/* REST PARAMETER SELECTORS                                                  */
/*****************************************************************************/

/** This indicates that we get a parameter from the query string.  If the
    parameter is not present, we throw an exception.
*/
template<typename T, typename Codec = RestCodec<T> >
struct RestParam {
    RestParam()
    {
    }

    RestParam(const std::string & name, const std::string & description,
              Codec codec = Codec())
        : name(name), description(description),
          codec(std::move(codec))
    {
        //std::cerr << "created RestParam with " << name << " at "
        //          << this << std::endl;
    }
    
    RestParam(const RestParam & other)
        : name(other.name), description(other.description), codec(other.codec)
    {
        //std::cerr << "copied RestParam with " << name << " to "
        //          << this << std::endl;
    }

    std::string name;
    std::string description;
    Codec codec;

private:
    void operator = (const RestParam & other);
};


/** This indicates that we get a parameter from the query string.  If the
    parameter is not present, we use a default value.
*/
template<typename T, typename Codec = RestCodec<T> >
struct RestParamDefault {
    RestParamDefault()
    {
    }

    RestParamDefault(const std::string & name,
                     const std::string & description,
                     T defaultValue,
                     const std::string & defaultValueStr,
                     Codec codec = Codec())
        : name(name), description(description), defaultValue(defaultValue),
          defaultValueStr(defaultValueStr),
          codec(std::move(codec))
    {
        //std::cerr << "created RestParam with " << name << " at "
        //          << this << std::endl;
    }

    RestParamDefault(const std::string & name,
                     const std::string & description,
                     T defaultValue = T(),
                     Codec codec = Codec())
        : name(name), description(description), defaultValue(defaultValue),
          defaultValueStr(codec.encode(defaultValue)),
          codec(std::move(codec))
    {
        //std::cerr << "created RestParam with " << name << " at "
        //          << this << std::endl;
    }
    
    RestParamDefault(const RestParamDefault & other)
        : name(other.name), description(other.description),
          defaultValue(other.defaultValue),
          defaultValueStr(other.defaultValueStr),
          codec(other.codec)
    {
        //std::cerr << "copied RestParam with " << name << " to "
        //          << this << std::endl;
    }

    std::string name;
    std::string description;
    T defaultValue;
    std::string defaultValueStr;
    Codec codec;

private:
    void operator = (const RestParamDefault & other);
};


/** This indicates that we get a parameter from the query string and decode
    it from JSON.  If the parameter is not present, we throw an exception.
*/
template<typename T, typename Codec = JsonStrCodec<T> >
struct RestParamJson : public RestParam<T, Codec> {
    RestParamJson()
    {
    }

    RestParamJson(const std::string & name, const std::string & description,
                  Codec codec = Codec())
        : RestParam<T, Codec>(name, description, std::move(codec))
    {
    }
    
private:
    void operator = (const RestParamJson & other);
};


/** This indicates that we get a parameter from the query string.  If the
    parameter is not present, we use a default value.  Encoding/decoding
    is done through JSON.
*/
template<typename T, typename Codec = JsonStrCodec<T> >
struct RestParamJsonDefault : public RestParamDefault<T, Codec> {
    RestParamJsonDefault()
    {
    }

    RestParamJsonDefault(const std::string & name,
                         const std::string & description,
                         T defaultValue = T(),
                         const std::string & defaultValueStr = "",
                         Codec codec = Codec())
        : RestParamDefault<T, Codec>(name, description, defaultValue,
                                     defaultValueStr, std::move(codec))
    {
    }
    
private:
    void operator = (const RestParamJsonDefault & other);
};


/** This indicates that we get a parameter from the JSON payload.
    If name is empty, then we get the whole payload.  Otherwise we
    get the named value of the payload.
*/
template<typename T>
struct JsonParam {
    JsonParam()
    {
    }

    JsonParam(const std::string & name, const std::string & description)
        : name(name), description(description)
    {
    }
    
    JsonParam(const JsonParam & other)
        : name(other.name), description(other.description)
    {
    }
    
    std::string name;
    std::string description;
};


/** This indicates that we get a parameter from the path of the request. 
    For example, GET /v1/object/3/value, this would be able to bind "3" to
    a parameter of the request.
*/
template<typename T, class Codec = RestCodec<T> >
struct RequestParam {
    RequestParam()
    {
    }

    RequestParam(int index, const std::string & name, const std::string & description)
        : index(index), name(name), description(description)
    {
    }

    RequestParam(const RequestParam & other)
        : index(other.index),
          name(other.name),
          description(other.description)
    {
    }

    int index;
    std::string name;
    std::string description;
};

/** Free function to take the payload and pass it as a string. */
struct StringPayload {
    StringPayload(const std::string & description)
        : description(description)
    {
    }

    std::string description;
};

struct PassConnectionId {
};

struct PassParsingContext {
};

struct PassRequest {
};

/** This means to extract an object of the given type from the given
    resource of the context, and return a reference to it.
*/
template<typename Value, int Index = -1>
struct ObjectExtractor {
};
        


} // namespace Datacratic
