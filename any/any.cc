/* any.cc
   Jeremy Barnes, 18 June 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Implementation of the Any class.
*/

#include "any.h"
#include "soa/types/basic_value_descriptions.h"


using namespace std;


namespace Datacratic {


/*****************************************************************************/
/* ANY                                                                       */
/*****************************************************************************/

static TypedAnyDescription payloadDesc;

Any
Any::
jsonDecodeStrTyped(const std::string & json)
{
    std::istringstream stream(json);
    StreamingJsonParsingContext context(json, json.c_str(),
                                        json.c_str() + json.size());
    Any ev;
    payloadDesc.parseJsonTyped(&ev, context);

    return std::move(ev);
}

Any
Any::
jsonDecodeTyped(const Json::Value & json)
{
    StructuredJsonParsingContext context(json);
    Any ev;
    payloadDesc.parseJsonTyped(&ev, context);

    return std::move(ev);
}

std::string
Any::
jsonEncodeStrTyped(const Any & val)
{
    std::ostringstream stream;
    StreamJsonPrintingContext context(stream);
    payloadDesc.printJson(&val, context);
    return std::move(stream.str());
}

Json::Value
Any::
jsonEncodeTyped(const Any & val)
{
    StructuredJsonPrintingContext context;
    payloadDesc.printJson(&val, context);
    return std::move(context.output);
}

namespace {

struct AnyRep {
    AnyRep()
        : repVersion(1)
    {
    }

    std::string typeName;
    int repVersion;
    std::string valueDescriptionType;
    std::string valueDescriptionVersion;
    std::string payload;  // JSON-encoded string
};

CREATE_STRUCTURE_DESCRIPTION(AnyRep);

AnyRepDescription::
AnyRepDescription()
{
    addField("rv", &AnyRep::repVersion,
             "Version of representation");
    addField("tn", &AnyRep::typeName,
             "Type of object in payload");
    addField("vdt",
             &AnyRep::valueDescriptionType,
             "Type of value description that encoded the payload");
    addField("vdv",
             &AnyRep::valueDescriptionVersion,
             "Version of value description that encoded the payload");
    addField("p", &AnyRep::payload,
             "Payload of watch event (actual event that happened)");
}

} // file scope


void
TypedAnyDescription::
parseJsonTyped(Any * val,
               JsonParsingContext & context) const
{
    static AnyRepDescription repDesc;

    if (context.isNull()) {
        context.expectNull();
        *val = Any();
        return;
    }

    AnyRep rep;
    repDesc.parseJson(&rep, context);
        
    // Get the default description for the type
    auto desc = ValueDescription::get(rep.typeName);

    std::shared_ptr<void> obj(desc->constructDefault(),
                              [=] (void * val) { desc->destroy(val); });
        
    StreamingJsonParsingContext pcontext(rep.payload,
                                         rep.payload.c_str(),
                                         rep.payload.size());
    desc->parseJson(obj.get(), pcontext);

    val->obj_ = std::move(obj);
    val->desc_ = desc.get();
    val->type_ = desc->type;
}

void
TypedAnyDescription::
printJsonTyped(const Any * val,
                            JsonPrintingContext & context) const
{
    static AnyRepDescription desc;

    if (val->empty()) {
        context.writeNull();
        return;
    }

    AnyRep rep;
    rep.typeName = val->type().name();
    rep.valueDescriptionType = typeid(val->desc()).name();

    std::ostringstream stream;
    StreamJsonPrintingContext pcontext(stream);
        
    val->desc().printJson(val->obj_.get(), pcontext);
    rep.payload = stream.str();

    desc.printJson(&rep, context);
}

ValueDescriptionT<Any> *
getDefaultDescription(Any *)
{
    return new BareAnyDescription();
}

/** Alternative value description for Any that only prints the JSON,
    not the type information.
    
    When reconstituting, no type information is kept (only the
    Json::Value).  This means that it will need to be converted to
    the underlying type.
*/
void
BareAnyDescription::
parseJsonTyped(Any * val,
               JsonParsingContext & context) const
{
    Json::Value jsonVal = context.expectJson();
    val->setJson(jsonVal);
}

void
BareAnyDescription::
printJsonTyped(const Any * val,
               JsonPrintingContext & context) const
{
    if (val->empty()) {
        context.writeNull();
        return;
    }
    if (val->type() == typeid(Json::Value)) {
        auto j = val->as<Json::Value>();
        context.writeJson(j);
        return;
    }

    val->desc().printJson(val->obj_.get(), context);
}

bool
BareAnyDescription::
isDefaultTyped(const Any * val) const
{
    return val->empty();
}

template class ValueDescriptionT<Any>;

} // namespace Datacratic
