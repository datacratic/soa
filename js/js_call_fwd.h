/* js_call_fwd.h                                                   -*- C++ -*-
   Jeremy Barnes, 16 November 2010
   Copyright (c) 2010 Datacratic.  All rights reserved.

   Forward definitions for the js_call infrastructure.
*/

#ifndef __js__js_call_fwd_h__
#define __js__js_call_fwd_h__

#include <functional>
#include <typeinfo>

namespace v8 {
struct Arguments;
template<typename T> struct Handle;
template<typename T> struct Persistent;
template<typename T> struct Local;
struct Value;
struct Function;
struct Object;

} // namespace v8

namespace Datacratic {
namespace JS {

/* Arity utility */
template <typename T>
struct fn_arity
{
};

template <typename R, typename... Args>
struct fn_arity<std::function<R(Args...)> >
{
    static constexpr int value = sizeof...(Args);
};

template <typename R, typename... Args>
struct fn_arity<R(Args...)>
{
    static constexpr int value = sizeof...(Args);
};


template<typename Fn, int arity = fn_arity<Fn >::value>
struct callfromjs;

template<typename Fn, int arity = fn_arity<Fn >::value>
struct calltojs;


struct JSArgs;

/** Operations function for Javascript.  Defined in js_call.h.
    
    Operation 0: call std function
        var1 = pointer to function
        var2 = pointer to JSArgs instance
        var3 = pointer to v8::Handle<v8::Value> for result

    Operation 1: convert to std::function
        var1 = pointer to v8::Handle<v8::Function> for function
        var2 = pointer to v8::Handle<v8::Object> for This
        var3 = pointer to std::function for result
*/
typedef void (*JSCallsStdFn) (int op,
                              void * fn,
                              const JS::JSArgs & args,
                              v8::Handle<v8::Value> & result);

typedef void (*JSAsStdFn) (int op,
                           const v8::Persistent<v8::Function> & fn,
                           const v8::Handle<v8::Object> & This,
                           void * result);

// This is compatible with the previous two
typedef void (*JSOperations) (int op,
                              const void * arg1,
                              const void * arg2,
                              void * result);

JSOperations getOps(const std::type_info & fntype);
void registerJsOps(const std::type_info & type,
                   JS::JSOperations ops);
    


} // namespace JS
} // namespace Datacratic

#endif /* __js__js_call_fwd_h__ */

