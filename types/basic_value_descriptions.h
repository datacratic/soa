/* basic_value_descriptions.h                                      -*- C++ -*-
   Jeremy Barnes, 20 March 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "value_description.h"
#include "soa/types/url.h"
#include "soa/types/date.h"


namespace Datacratic {

/*****************************************************************************/
/* DEFAULT DESCRIPTIONS FOR BASIC TYPES                                      */
/*****************************************************************************/

template<>
struct DefaultDescription<Datacratic::Id>
    : public ValueDescriptionI<Datacratic::Id, ValueKind::ATOM> {

    virtual void parseJsonTyped(Datacratic::Id * val,
                                JsonParsingContext & context) const
    {
        Datacratic::parseJson(val, context);
    }

    virtual void printJsonTyped(const Datacratic::Id * val,
                                JsonPrintingContext & context) const
    {
        if (val->val2 == 0 && val->type == Id::Type::BIGDEC) {
            context.writeUnsignedLongLong(val->toInt());
        }
        else {
            context.writeString(val->toString());
        }
    }

    virtual bool isDefaultTyped(const Datacratic::Id * val) const
    {
        return !val->notNull();
    }
};

template<>
struct DefaultDescription<std::string>
    : public ValueDescriptionI<std::string, ValueKind::STRING> {

    virtual void parseJsonTyped(std::string * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectStringAscii();
    }

    virtual void printJsonTyped(const std::string * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(*val);
    }

    virtual bool isDefaultTyped(const std::string * val) const
    {
        return val->empty();
    }
};

template<>
struct DefaultDescription<Utf8String>
    : public ValueDescriptionI<Utf8String, ValueKind::STRING> {

    virtual void parseJsonTyped(Utf8String * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectStringUtf8();
    }

    virtual void printJsonTyped(const Utf8String * val,
                                JsonPrintingContext & context) const
    {
        context.writeStringUtf8(*val);
    }

    virtual bool isDefaultTyped(const Utf8String * val) const
    {
        return val->empty();
    }
};

template<>
struct DefaultDescription<Url>
    : public ValueDescriptionI<Url, ValueKind::ATOM> {

    virtual void parseJsonTyped(Url * val,
                                JsonParsingContext & context) const
    {
        *val = Url(context.expectStringUtf8());
    }

    virtual void printJsonTyped(const Url * val,
                                JsonPrintingContext & context) const
    {
        context.writeStringUtf8(val->toUtf8String());
    }

    virtual bool isDefaultTyped(const Url * val) const
    {
        return val->empty();
    }
};

template<>
struct DefaultDescription<signed int>
    : public ValueDescriptionI<signed int, ValueKind::INTEGER> {

    virtual void parseJsonTyped(signed int * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const signed int * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template<>
struct DefaultDescription<unsigned int>
    : public ValueDescriptionI<unsigned int, ValueKind::INTEGER> {

    virtual void parseJsonTyped(unsigned int * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectInt();
    }
    
    virtual void printJsonTyped(const unsigned int * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(*val);
    }
};

template<>
struct DefaultDescription<signed long>
    : public ValueDescriptionI<signed long, ValueKind::INTEGER> {

    virtual void parseJsonTyped(signed long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectLong();
    }
    
    virtual void printJsonTyped(const signed long * val,
                                JsonPrintingContext & context) const
    {
        context.writeLong(*val);
    }
};

template<>
struct DefaultDescription<unsigned long>
    : public ValueDescriptionI<unsigned long, ValueKind::INTEGER> {

    virtual void parseJsonTyped(unsigned long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectUnsignedLong();
    }
    
    virtual void printJsonTyped(const unsigned long * val,
                                JsonPrintingContext & context) const
    {
        context.writeUnsignedLong(*val);
    }
};

template<>
struct DefaultDescription<signed long long>
    : public ValueDescriptionI<signed long long, ValueKind::INTEGER> {

    virtual void parseJsonTyped(signed long long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectLongLong();
    }
    
    virtual void printJsonTyped(const signed long long * val,
                                JsonPrintingContext & context) const
    {
        context.writeLongLong(*val);
    }
};

template<>
struct DefaultDescription<unsigned long long>
    : public ValueDescriptionI<unsigned long long, ValueKind::INTEGER> {

    virtual void parseJsonTyped(unsigned long long * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectUnsignedLongLong();
    }
    
    virtual void printJsonTyped(const unsigned long long * val,
                                JsonPrintingContext & context) const
    {
        context.writeUnsignedLongLong(*val);
    }
};

template<>
struct DefaultDescription<float>
    : public ValueDescriptionI<float, ValueKind::FLOAT> {

    virtual void parseJsonTyped(float * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectFloat();
    }

    virtual void printJsonTyped(const float * val,
                                JsonPrintingContext & context) const
    {
        context.writeFloat(*val);
    }
};

template<>
struct DefaultDescription<double>
    : public ValueDescriptionI<double, ValueKind::FLOAT> {

    virtual void parseJsonTyped(double * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectDouble();
    }

    virtual void printJsonTyped(const double * val,
                                JsonPrintingContext & context) const
    {
        context.writeDouble(*val);
    }
};

#if 0
template<typename T>
struct DefaultDescription<std::vector<T> >
    : public ValueDescriptionI<std::vector<T>, ValueKind::ARRAY> {
};
#endif

template<typename T>
struct DefaultDescription<std::unique_ptr<T> >
    : public ValueDescriptionI<std::unique_ptr<T>, ValueKind::OPTIONAL> {

    DefaultDescription(ValueDescriptionT<T> * inner
                       = getDefaultDescription((T *)0))
        : inner(inner)
    {
    }

    std::unique_ptr<ValueDescriptionT<T> > inner;

    virtual void parseJsonTyped(std::unique_ptr<T> * val,
                                JsonParsingContext & context) const
    {
        val->reset(new T());
        inner->parseJsonTyped(val->get(), context);
    }

    virtual void printJsonTyped(const std::unique_ptr<T> * val,
                                JsonPrintingContext & context) const
    {
        if (!val->get())
            context.skip();
        else inner->printJsonTyped(val->get(), context);
    }

    virtual bool isDefaultTyped(const std::unique_ptr<T> * val) const
    {
        return !val->get();
    }
};

template<typename T>
struct DefaultDescription<std::shared_ptr<T> >
    : public ValueDescriptionI<std::shared_ptr<T>, ValueKind::OPTIONAL> {

    DefaultDescription(ValueDescriptionT<T> * inner
                       = getDefaultDescription((T *)0))
        : inner(inner)
    {
    }

    std::unique_ptr<ValueDescriptionT<T> > inner;

    virtual void parseJsonTyped(std::shared_ptr<T> * val,
                                JsonParsingContext & context) const
    {
        val->reset(new T());
        inner->parseJsonTyped(val->get(), context);
    }

    virtual void printJsonTyped(const std::shared_ptr<T> * val,
                                JsonPrintingContext & context) const
    {
        if (!val->get())
            context.skip();
        else inner->printJsonTyped(val->get(), context);
    }

    virtual bool isDefaultTyped(const std::shared_ptr<T> * val) const
    {
        return !val->get();
    }
};

template<>
struct DefaultDescription<Json::Value>
    : public ValueDescriptionI<Json::Value, ValueKind::ANY> {

    virtual void parseJsonTyped(Json::Value * val,
                                JsonParsingContext & context) const
    {
        Datacratic::parseJson(val, context);
    }

    virtual void printJsonTyped(const Json::Value * val,
                                JsonPrintingContext & context) const
    {
        context.writeJson(*val);
    }

    virtual bool isDefaultTyped(const Json::Value * val) const
    {
        return val->isNull();
    }
};

template<>
struct DefaultDescription<bool>
    : public ValueDescriptionI<bool, ValueKind::BOOLEAN> {

    virtual void parseJsonTyped(bool * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectBool();
    }

    virtual void printJsonTyped(const bool * val,
                                JsonPrintingContext & context) const
    {
        context.writeBool(*val);
    }

    virtual bool isDefaultTyped(const bool * val) const
    {
        return false;
    }
};

template<>
struct DefaultDescription<Date>
    : public ValueDescriptionI<Date, ValueKind::ATOM> {

    virtual void parseJsonTyped(Date * val,
                                JsonParsingContext & context) const
    {
        if (context.isNumber())
            *val = Date::fromSecondsSinceEpoch(context.expectDouble());
        else if (context.isString())
            *val = Date::parseDefaultUtc(context.expectStringAscii());
        else context.exception("expected date");
    }

    virtual void printJsonTyped(const Date * val,
                                JsonPrintingContext & context) const
    {
        context.writeJson(val->secondsSinceEpoch());
    }

    virtual bool isDefaultTyped(const Date * val) const
    {
        return *val == Date();
    }
};

template<typename T>
struct Optional: public std::unique_ptr<T> {
    Optional()
    {
    }
    
    Optional(Optional && other)
        : std::unique_ptr<T>(std::move(other))
    {
    }

    Optional(const Optional & other)
    {
        if (other)
            this->reset(new T(*other));
    }

    Optional & operator = (const Optional & other)
    {
        Optional newMe(other);
        swap(newMe);
        return *this;
    }

    Optional & operator = (Optional && other)
    {
        Optional newMe(other);
        swap(newMe);
        return *this;
    }

    void swap(Optional & other)
    {
        std::unique_ptr<T>::swap(other);
    }
};

template<typename Cls, int defValue = -1>
struct TaggedEnum {
    TaggedEnum()
        : val(-1)
    {
    }

    int val;

    int value() const
    {
        return val;
    }


#if 0
    operator typename Cls::Vals () const
    {
        return static_cast<typename Cls::Vals>(val);
    }
#endif
};

template<typename E, int def>
bool operator == (const TaggedEnum<E, def> & e1, const TaggedEnum<E, def> & e2)
{
    return e1.val == e2.val;
}

template<typename E, int def>
bool operator != (const TaggedEnum<E, def> & e1, const TaggedEnum<E, def> & e2)
{
    return e1.val != e2.val;
}

template<typename E, int def>
bool operator > (const TaggedEnum<E, def> & e1, const TaggedEnum<E, def> & e2)
{
    return e1.val > e2.val;
}

template<typename E, int def>
bool operator < (const TaggedEnum<E, def> & e1, const TaggedEnum<E, def> & e2)
{
    return e1.val < e2.val;
}

template<typename E, int def>
inline Json::Value jsonPrint(const TaggedEnum<E, def> & e)
{
    return e.val;
}

template<typename E, int def>
inline void jsonParse(const Json::Value & j, TaggedEnum<E, def> & e)
{
    e.val = j.asInt();
}

struct TaggedBool {
    TaggedBool()
        : val(-1)
    {
    }

    int val;
};

template<int defValue = -1>
struct TaggedBoolDef : public TaggedBool {
    TaggedBoolDef()
        : val(defValue)
    {
    }

    int val;
};

struct TaggedInt {
    TaggedInt()
        : val(-1)
    {
    }

    int value() const { return val; }

    int val;
};

template<int defValue = -1>
struct TaggedIntDef : TaggedInt {
    TaggedIntDef()
        : val(defValue)
    {
    }

    int val;
};

struct TaggedInt64 {
    TaggedInt64()
        : val(-1)
    {
    }

    int64_t value() const { return val; }

    int64_t val;
};

template<int64_t defValue = -1>
struct TaggedInt64Def : TaggedInt {
    TaggedInt64Def()
        : val(defValue)
    {
    }

    int64_t val;
};

struct TaggedFloat {
    TaggedFloat()
        : val(std::numeric_limits<float>::quiet_NaN())
    {
    }

    float val;
};

template<int num = -1, int den = 1>
struct TaggedFloatDef : public TaggedFloat {
    TaggedFloatDef()
        : val(1.0f * num / den)
    {
    }

    float val;
};

struct TaggedDouble {
    TaggedDouble()
        : val(std::numeric_limits<double>::quiet_NaN())
    {
    }

    double val;
};

template<int num = -1, int den = 1>
struct TaggedDoubleDef : public TaggedDouble {
    TaggedDoubleDef()
        : val(1.0 * num / den)
    {
    }

    double val;
};

template<>
struct DefaultDescription<TaggedBool>
    : public ValueDescriptionI<TaggedBool, ValueKind::BOOLEAN> {
  
    virtual void parseJsonTyped(TaggedBool * val,
                                JsonParsingContext & context) const
    {
        if (context.isBool())
            val->val = context.expectBool();
        else val->val = context.expectInt();
    }

    virtual void printJsonTyped(const TaggedBool * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(val->val);
    }

    virtual bool isDefaultTyped(const TaggedBool * val)
        const
    {
        return val->val == -1;
    }
};

template<int defValue>
struct DefaultDescription<TaggedBoolDef<defValue> >
    : public ValueDescriptionI<TaggedBoolDef<defValue>,
                               ValueKind::BOOLEAN> {
  
    virtual void parseJsonTyped(TaggedBoolDef<defValue> * val,
                                JsonParsingContext & context) const
    {
        if (context.isBool())
            val->val = context.expectBool();
        else val->val = context.expectInt();
    }

    virtual void printJsonTyped(const TaggedBoolDef<defValue> * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(val->val);
    }

    virtual bool isDefaultTyped(const TaggedBoolDef<defValue> * val)
        const
    {
        return val->val == defValue;
    }
};

template<>
struct DefaultDescription<TaggedInt>
    : public ValueDescriptionI<TaggedInt,
                               ValueKind::INTEGER,
                               DefaultDescription<TaggedInt> > {

    virtual void parseJsonTyped(TaggedInt * val,
                                JsonParsingContext & context) const
    {
        if (context.isString()) {
            std::string s = context.expectStringAscii();
            val->val = boost::lexical_cast<int>(s);
        }
        else val->val = context.expectInt();
    }

    virtual void printJsonTyped(const TaggedInt * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(val->val);
    }

    virtual bool isDefaultTyped(const TaggedInt * val)
        const
    {
        return val->val == -1;
    }
};

template<int defValue>
struct DefaultDescription<TaggedIntDef<defValue> >
    : public ValueDescriptionI<TaggedIntDef<defValue>,
                               ValueKind::INTEGER> {

    virtual void parseJsonTyped(TaggedIntDef<defValue> * val,
                                JsonParsingContext & context) const
    {
        if (context.isString()) {
            std::string s = context.expectStringAscii();
            val->val = boost::lexical_cast<int>(s);
        }
        else val->val = context.expectInt();
    }

    virtual void printJsonTyped(const TaggedIntDef<defValue> * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(val->val);
    }

    virtual bool isDefaultTyped(const TaggedIntDef<defValue> * val)
        const
    {
        return val->val == defValue;
    }
};

template<>
struct DefaultDescription<TaggedInt64>
: public ValueDescriptionI<TaggedInt64,
                           ValueKind::INTEGER,
                           DefaultDescription<TaggedInt64> > {
    
    virtual void parseJsonTyped(TaggedInt64 * val,
                                JsonParsingContext & context) const
    {
        if (context.isString()) {
            std::string s = context.expectStringAscii();
            val->val = boost::lexical_cast<int64_t>(s);
        }
        else val->val = context.expectLongLong();
    }

    virtual void printJsonTyped(const TaggedInt64 * val,
                                JsonPrintingContext & context) const
    {
        context.writeLongLong(val->val);
    }

    virtual bool isDefaultTyped(const TaggedInt64 * val) const
    {
        return val->val == -1;
    }
};

template<int64_t defValue>
struct DefaultDescription<TaggedInt64Def<defValue> >
  : public ValueDescriptionI<TaggedInt64Def<defValue>, ValueKind::INTEGER > {

    virtual void parseJsonTyped(TaggedInt64Def<defValue> * val,
                                JsonParsingContext & context) const
    {
        if (context.isString()) {
            std::string s = context.expectStringAscii();
            val->val = boost::lexical_cast<int64_t>(s);
        }
        else val->val = context.expectLongLong();
    }

    virtual void printJsonTyped(const TaggedInt64Def<defValue> * val,
                                JsonPrintingContext & context) const
    {
        context.writeLongLong(val->val);
    }

    virtual bool isDefaultTyped(const TaggedInt64Def<defValue> * val) const
    {
        return val->val == defValue;
    }
};

template<>
struct DefaultDescription<TaggedFloat>
    : public ValueDescriptionI<TaggedFloat,
                               ValueKind::FLOAT> {

    virtual void parseJsonTyped(TaggedFloat * val,
                                JsonParsingContext & context) const
    {
        val->val = context.expectFloat();
    }

    virtual void printJsonTyped(const TaggedFloat * val,
                                JsonPrintingContext & context) const
    {
        context.writeDouble(val->val);
    }

    virtual bool isDefaultTyped(const TaggedFloat * val) const
    {
        return isnan(val->val);
    }
};

template<int num, int den>
struct DefaultDescription<TaggedFloatDef<num, den> >
    : public ValueDescriptionI<TaggedFloatDef<num, den>,
                               ValueKind::FLOAT> {

    virtual void parseJsonTyped(TaggedFloatDef<num, den> * val,
                                JsonParsingContext & context) const
    {
        val->val = context.expectFloat();
    }

    virtual void printJsonTyped(const TaggedFloatDef<num, den> * val,
                                JsonPrintingContext & context) const
    {
        context.writeFloat(val->val);
    }

    virtual bool isDefaultTyped(const TaggedFloatDef<num, den> * val) const
    {
        return val->val == (float)num / den;
    }
};

template<>
struct DefaultDescription<TaggedDouble>
    : public ValueDescriptionI<TaggedDouble,
                               ValueKind::FLOAT> {

    virtual void parseJsonTyped(TaggedDouble * val,
                                JsonParsingContext & context) const
    {
        val->val = context.expectDouble();
    }

    virtual void printJsonTyped(const TaggedDouble * val,
                                JsonPrintingContext & context) const
    {
        context.writeDouble(val->val);
    }

    virtual bool isDefaultTyped(const TaggedDouble * val) const
    {
        return isnan(val->val);
    }
};

template<int num, int den>
struct DefaultDescription<TaggedDoubleDef<num, den> >
    : public ValueDescriptionI<TaggedDoubleDef<num, den>,
                               ValueKind::FLOAT> {

    virtual void parseJsonTyped(TaggedDoubleDef<num, den> * val,
                                JsonParsingContext & context) const
    {
        val->val = context.expectDouble();
    }

    virtual void printJsonTyped(const TaggedDoubleDef<num, den> * val,
                                JsonPrintingContext & context) const
    {
        context.writeDouble(val->val);
    }

    virtual bool isDefaultTyped(const TaggedDoubleDef<num, den> * val) const
    {
        return val->val == (double)num / den;
    }
};

template<class Enum>
struct TaggedEnumDescription
    : public ValueDescriptionI<Enum, ValueKind::ENUM,
                               TaggedEnumDescription<Enum> > {

    virtual void parseJsonTyped(Enum * val,
                                JsonParsingContext & context) const
    {
        int index = context.expectInt();
        val->val = index;
    }

    virtual void printJsonTyped(const Enum * val,
                                JsonPrintingContext & context) const
    {
        context.writeInt(val->val);
    }

    virtual bool isDefaultTyped(const Enum * val) const
    {
        return val->val == -1;
    }
};

} // namespace Datacratic
