/* any.h                                                           -*- C++ -*-

   Jeremy Barnes, July 4 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.
*/

#include "soa/types/value_description.h"
//#include "jml/arch/rtti_utils.h"

#pragma once

namespace Datacratic {

/*****************************************************************************/
/* ANY                                                                       */
/*****************************************************************************/

/** Similar to boost::any, but uses a ValueDescription to do its job.  This
    allows, amongst other things, serialization to and from JSON.
*/

struct Any {

    Any() noexcept
        : type_(nullptr), desc_(nullptr)
    {
    }

    template<typename T>
    Any(const T & val,
        const ValueDescription * desc = getDefaultDescriptionShared((T *)0).get(),
        typename std::enable_if<!std::is_same<T, Any>::value>::type * = 0)
        : obj_(new T(val)),
          type_(&typeid(val)),
          desc_(desc)
    {
    }

    /** Construct directly from Json, with a known type */
    Any(const Json::Value & val,
        const ValueDescription * desc)
        : type_(desc->type),
          desc_(desc)
    {
        StructuredJsonParsingContext context(val);
        obj_.reset(desc->constructDefault(), [=] (void * obj) { desc->destroy(obj); });
        desc_->parseJson(obj_.get(), context);
    }

    /** Construct directly from Json */
    Any(const std::string & jsonValString,
        const ValueDescription * desc)
        : type_(desc->type),
          desc_(desc)
    {
        std::istringstream stream(jsonValString);
        StreamingJsonParsingContext context(jsonValString, stream);
        obj_.reset(desc->constructDefault(), [=] (void * obj) { desc->destroy(obj); });
        desc_->parseJson(obj_.get(), context);
    }

#if 0  // confuses the compiler for an Any copy
    template<typename T>
    Any(T && val,
        const ValueDescription * desc = getDefaultDescriptionShared((typename std::decay<T>::type *)0).get(),
        typename std::enable_if<!std::is_same<typename std::decay<T>::type, Any>::value>::type * = 0)
        : obj_(new typename std::decay<T>::type(std::move(val))),
          type_(&typeid(T)),
          desc_(desc)
    {
    }

    Any(Any && other)
        : obj_(std::move(other.obj_)),
          type_(other.type_),
          desc_(std::move(other.desc_))
    {
    }

#endif

    Any(const Any & other)
        : obj_(other.obj_),
          type_(other.type_),
          desc_(other.desc_)
    {
    }

    Any(std::nullptr_t)
        : type_(nullptr), desc_(nullptr)
    {
    }

    void swap(Any & other) noexcept
    {
        std::swap(obj_, other.obj_);
        std::swap(type_, other.type_);
        std::swap(desc_, other.desc_);
    }

    /** Decode an object returned from the typed Any serialization. */
    static Any jsonDecodeStrTyped(const std::string & json);

    /** Decode an object returned from the typed Any serialization. */
    static Any jsonDecodeTyped(const Json::Value & json);

    static std::string jsonEncodeStrTyped(const Any & val);

    static Json::Value jsonEncodeTyped(const Any & val);

    template<typename T>
    bool is() const
    {
        return type_ == &typeid(T);
    }

    template<typename T>
    const T & as() const
    {
        if (!type_)
            throw ML::Exception("bad Any cast: null value can't convert to '%s'",
                                ML::type_name<T>().c_str());

        // If the same type, conversion is trivial
        if (type_ == &typeid(T))
            return *reinterpret_cast<T *>(obj_.get());

        // Otherwise, go into RTTI and see if we can make it happen
        //const void * res = ML::is_convertible(*type_, typeid(T), obj_.get());
        //if (res)
        //    return *reinterpret_cast<const T *>(res);

        // Otherwise, no conversion is possible
        throw ML::Exception("bad Any cast: requested '%s', contained '%s'",
                            ML::type_name<T>().c_str(),
                            ML::demangle(*type_).c_str());
    }

    template<typename T>
    T convert(const ValueDescription & desc = *getDefaultDescriptionShared<T>()) const
    {
        if (!type_)
            throw ML::Exception("bad Any cast: null value can't convert to '%s'",
                                ML::type_name<T>().c_str());

        // If the same type, conversion is trivial
        if (type_ == &typeid(T))
            return *reinterpret_cast<T *>(obj_.get());

        // Otherwise, go into RTTI and see if we can make it happen
        //const void * res = ML::is_convertible(*type_, typeid(T), obj_.get());
        //if (res)
        //    return *reinterpret_cast<const T *>(res);

        if (type_ == &typeid(Json::Value)) {
            T result;
            StructuredJsonParsingContext context(*reinterpret_cast<const Json::Value *>(obj_.get()));
            desc.parseJson(&result, context);
            return result;
        }

        // Otherwise, no conversion is possible
        throw ML::Exception("bad Any conversion: requested '%s', contained '%s'",
                            ML::type_name<T>().c_str(),
                            ML::demangle(*type_).c_str());
    }

    bool empty() const
    {
        return !obj_;
    }

    const std::type_info & type() const
    {
        if (type_)
            return *type_;
        return typeid(void);
    }
    
    const ValueDescription & desc() const
    {
        if (!desc_)
            throw ML::Exception("no value description");
        return *desc_;
    }

    /** Assign a new value of the same type. */
    template<typename T>
    void assign(const T & value)
    {
        if (&typeid(T) == type_) {
            // Keep the same description, etc
            obj_.reset(new T(value));
        }
        else {
            *this = Any(value);
        }
    }

    /** Assign a new value of the same type. */
    template<typename T>
    void assign(T && value)
    {
        if (&typeid(T) == type_) {
            // Keep the same description, etc
            obj_.reset(new T(std::move(value)));
        }
        else {
            *this = Any(value);
        }
    }

    /** Get it as JSON */
    Json::Value asJson() const
    {
        Json::Value result;
        StructuredJsonPrintingContext context;
        if (!desc_) return result;
        //throw ML::Exception("cannot get value without value description as JSON");
        desc_->printJson(obj_.get(), context);
        return context.output;
    }

    /** Get it as stringified JSON */
    std::string asJsonStr() const
    {
        if (!desc_)
            return "null";
        std::ostringstream stream;
        StreamJsonPrintingContext context(stream);
        desc_->printJson(obj_.get(), context);
        return stream.str();
    }

    void setJson(const Json::Value & val)
    {
        obj_.reset(new Json::Value(val));
        desc_ = nullptr;
        type_ = &typeid(Json::Value);
    }

    void setJson(Json::Value && val)
    {
        obj_.reset(new Json::Value(std::move(val)));
        desc_ = nullptr;
        type_ = &typeid(Json::Value);
    }

private:
    friend class TypedAnyDescription;
    friend class BareAnyDescription;
    std::shared_ptr<void> obj_;
    const std::type_info * type_;
    const ValueDescription * desc_;

    /** Conversion function. */
    void convert(void * result, const std::type_info & toType);
};

extern template class ValueDescriptionT<Any>;

struct TypedAnyDescription: public ValueDescriptionT<Any> {
    virtual void parseJsonTyped(Any * val,
                                JsonParsingContext & context) const;
    virtual void printJsonTyped(const Any * val,
                                JsonPrintingContext & context) const;
};

ValueDescriptionT<Any> *
getDefaultDescription(Any * = 0);

/** Alternative value description for Any that only prints the JSON,
    not the type information.  This can't be used to reconstitute.
*/
struct BareAnyDescription: public ValueDescriptionT<Any> {
    virtual void parseJsonTyped(Any * val,
                                JsonParsingContext & context) const;
    virtual void printJsonTyped(const Any * val,
                                JsonPrintingContext & context) const;
    virtual bool isDefaultTyped(const Any * val) const;
};


} // namespace Datacratic
