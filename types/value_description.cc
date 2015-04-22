/* value_description.cc                                            -*- C++ -*-
   Jeremy Barnes, 29 March 2013
   Copyright (c) 2013 Datacratic Inc.  All rights reserved.

   Code for description and introspection of values and structures.  Used
   to allow for automated formatters and parsers to be built.
*/


#include <mutex>
#if 0
#include "jml/arch/demangle.h"
#endif
#include "jml/utils/exc_assert.h"
#include "value_description.h"

using namespace std;
using namespace ML;


namespace Datacratic {

std::ostream & operator << (std::ostream & stream, ValueKind kind)
{
    switch (kind) {
    case ValueKind::ATOM: return stream << "ATOM";
    case ValueKind::INTEGER: return stream << "INTEGER";
    case ValueKind::FLOAT: return stream << "FLOAT";
    case ValueKind::BOOLEAN: return stream << "BOOLEAN";
    case ValueKind::STRING: return stream << "STRING";
    case ValueKind::ENUM: return stream << "ENUM";
    case ValueKind::OPTIONAL: return stream << "OPTIONAL";
    case ValueKind::ARRAY: return stream << "ARRAY";
    case ValueKind::STRUCTURE: return stream << "STRUCTURE";
    case ValueKind::TUPLE: return stream << "TUPLE";
    case ValueKind::VARIANT: return stream << "VARIANT";
    case ValueKind::MAP: return stream << "MAP";
    case ValueKind::ANY: return stream << "ANY";
    default:
        return stream << "ValueKind(" << std::to_string((int)kind) << ")";
    }
}

namespace {
std::recursive_mutex registryMutex;
std::unordered_map<std::string, std::shared_ptr<const ValueDescription> > registry;
}

std::shared_ptr<const ValueDescription>
ValueDescription::
get(std::string const & name)
{
    std::unique_lock<std::recursive_mutex> guard(registryMutex);
    auto i = registry.find(name);
    return registry.end() != i ? i->second : nullptr;
}

std::shared_ptr<const ValueDescription>
ValueDescription::
get(const std::type_info & type)
{
    return get(type.name());
}

void registerValueDescription(const std::type_info & type,
                              std::function<ValueDescription * ()> fn,
                              bool isDefault)
{
    registerValueDescription(type, fn, [] (ValueDescription &) {}, isDefault);
}

void
registerValueDescription(const std::type_info & type,
                         std::function<ValueDescription * ()> createFn,
                         std::function<void (ValueDescription &)> initFn,
                         bool isDefault)
{
    std::unique_lock<std::recursive_mutex> guard(registryMutex);

    std::shared_ptr<ValueDescription> desc(createFn());
    ExcAssert(desc);
    registry[desc->typeName] = desc;
    registry[type.name()] = desc;

    initFn(*desc);

#if 0
    cerr << "type " << ML::demangle(type.name())
         << " has description "
         << ML::type_name(*desc) << " default " << isDefault << endl;

    if (registry.count(type.name()))
        throw ML::Exception("attempt to double register "
                            + ML::demangle(type.name()));
#endif
}

void
ValueDescription::
convertAndCopy(const void * from,
               const ValueDescription & fromDesc,
               void * to) const
{
    StructuredJsonPrintingContext context;
    fromDesc.printJson(from, context);

    StructuredJsonParsingContext context2(context.output);
    parseJson(to, context2);
}

/*****************************************************************************/
/* STRUCTURE DESCRIPTION BASE                                                */
/*****************************************************************************/


StructureDescriptionBase::
StructureDescriptionBase(const std::type_info * type,
                         ValueDescription * owner,
                         const std::string & structName,
                         bool nullAccepted)
    : type(type),
      structName(structName.empty() ? ML::demangle(type->name()) : structName),
      nullAccepted(nullAccepted),
      owner(owner)
{
}

void
StructureDescriptionBase::
operator = (const StructureDescriptionBase & other)
{
    type = other.type;
    structName = other.structName;
    nullAccepted = other.nullAccepted;

    fieldNames.clear();
    orderedFields.clear();
    fields.clear();
    fieldNames.reserve(other.fields.size());

    // Don't set owner
    for (auto & f: other.orderedFields) {
        string s = f->first;
        fieldNames.push_back(s);
        auto it = fields.insert(make_pair(fieldNames.back().c_str(), f->second))
            .first;
        orderedFields.push_back(it);
    }
}

void
StructureDescriptionBase::
operator = (StructureDescriptionBase && other)
{
    type = std::move(other.type);
    structName = std::move(other.structName);
    nullAccepted = std::move(other.nullAccepted);
    fields = std::move(other.fields);
    fieldNames = std::move(other.fieldNames);
    orderedFields = std::move(other.orderedFields);
    // don't set owner
}

StructureDescriptionBase::Exception::
Exception(JsonParsingContext & context,
          const std::string & message)
    : ML::Exception("at " + context.printPath() + ": " + message)
{
}

StructureDescriptionBase::Exception::
~Exception() throw ()
{
}

void
StructureDescriptionBase::
parseJson(void * output, JsonParsingContext & context) const
{
    try {

        if (!onEntry(output, context)) return;

        if (nullAccepted && context.isNull()) {
            context.expectNull();
            return;
        }
        
        if (!context.isObject()) {
            std::string typeName;
            if (context.isNumber())
                typeName = "number";
            else if (context.isBool())
                typeName = "boolean";
            else if (context.isString())
                typeName = "string";
            else if (context.isNull())
                typeName = "null";
            else if (context.isArray())
                typeName = "array";
            else typeName = "<<unknown type>>";
                    
            std::string msg
                = "expected object of type "
                + structName + ", but instead a "
                + typeName + " was provided";

            if (context.isString())
                msg += ".  Did you accidentally JSON encode your object into a string?";

            context.exception(msg);
        }

        auto onMember = [&] ()
            {
                try {
                    auto n = context.fieldNamePtr();
                    auto it = fields.find(n);
                    if (it == fields.end()) {
                        for (auto & f: fields) {
                            using namespace std;
                            cerr << "known field " << f.first << endl;
                        }
                        context.onUnknownField(owner);
                    }
                    else {
                        it->second.description
                        ->parseJson(addOffset(output,
                                              it->second.offset),
                                    context);
                    }
                }
                catch (const Exception & exc) {
                    throw;
                }
                catch (const std::exception & exc) {
                    throw Exception(context, exc.what());
                }
                catch (...) {
                    throw;
                }
            };

        context.forEachMember(onMember);

        onExit(output, context);
    }
    catch (const Exception & exc) {
        throw;
    }
    catch (const std::exception & exc) {
        throw Exception(context, exc.what());
    }
    catch (...) {
        throw;
    }
}

void
StructureDescriptionBase::
printJson(const void * input, JsonPrintingContext & context) const
{
    context.startObject();

    for (const auto & it: orderedFields) {
        auto & fd = it->second;

        auto mbr = addOffset(input, fd.offset);
        if (fd.description->isDefault(mbr))
            continue;
        context.startMember(it->first);
        fd.description->printJson(mbr, context);
    }
        
    context.endObject();
}

} // namespace Datacratic
