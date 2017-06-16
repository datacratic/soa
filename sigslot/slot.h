/* slot.h                                                          -*- C++ -*-
   Jeremy Barnes, 16 November 2010
   Copyright (c) 2010 Datacratic.  All rights reserved.

   Implementation of a "slot" object that can be attached to a signal.
*/

#pragma once

#include <functional>

#include "jml/arch/demangle.h"
#include "jml/arch/exception.h"
#include "jml/utils/exc_assert.h"

#if NODEJS_ENABLED
#include "soa/js/js_call_fwd.h"
#endif // NODEJS_ENABLED

namespace boost {
namespace signals2 {
struct connection;
} // namespace signals2
} // namespace boost

namespace Json {
struct Value;
} // namespace Json

namespace Datacratic {

#if NODEJS_ENABLED
bool inJsContext();

void enterJs(void * & locker);
void exitJs(void * & locker);

struct JSLocker {
    JSLocker()
    {
        enterJs(locker);
    }
    
    ~JSLocker()
    {
        exitJs(locker);
    }
    
    void * locker;
};
#endif // NODEJS_ENABLED

/** Type of a function used to disconnect a slot from a signal. */
struct SlotDisconnector
    : public std::function<void (void)> {
    
    template<typename T>
    SlotDisconnector(const T & f)
        : std::function<void (void)>(f)
    {
    }

    SlotDisconnector()
    {
    }

    SlotDisconnector(const boost::signals2::connection & connection);
};

/* What do we want to do here?
   
   We want to be able to:
   - Pass std::function objects of different types around through the one
     interface
   - Call them as a specific type of std::function when we need to
   - Have them throw an exception if we try to call them as something that
     they're not
   - Be able to initialize them from javascript functions in such a way that
     everything will be forwarded automatically
   - Be able to make them callable from javascript
*/

/** Type of the function that does the heavy lifting.  It's a multiplexed
    function (behind the argument).

    Operations:
    0:   returns the pointer to the std::type_info block for the operator
         class.  arg1 and arg2 are unused.
    1:   free the std::function_base descendent pointed to by arg1
    3:   copy the std::function_base descendent pointed to by arg1 and
         return a pointer to the new copy
*/
typedef void * (* Operations) (int op, void * arg);

/** Default implementation of the operations on the function. */
template<typename Fn>
struct FunctionOps {
    typedef typename std::function<Fn> Function;

    static void * ops(int op, void * arg)
    {
        switch (op) {
        case 0: *(const std::type_info **)(arg) = &typeid(Fn);  break;
        case 1: delete (std::function<Fn> *)arg;  break;
        case 3: return new Function(*(std::function<Fn> *)arg);
        default:
            throw ML::Exception("invalid operation number");
        }
        return 0;
    }
};


/*****************************************************************************/
/* SLOT                                                                      */
/*****************************************************************************/

/** Encapsulates a function that can be called with a specific argument list,
    without knowing the actual argument list.

    Note that in the C++ world, the signatures under which we are calling and
    the signature of the wrapped function must match exactly (ie, no type
    coersion.

    These can be:
    - Manipulated and used without knowing the exact type signature of the
      event;
    - Made to point to a Javascript function that will be called with the
      arguments forwarded from C++;
    - Called, queried and hooked up from Javascript
*/

struct Slot {
    Slot()
        : fn(nullptr), fntype(EMPTY)
    {
    }

    Slot(Slot && other);

    Slot & operator = (Slot && other);

    void swap(Slot & other);

    Slot(const Slot & other);

    Slot & operator = (const Slot & other);

    // Initialize from a std::function where the function type and the
    // called type are identical
#if NODEJS_ENABLED
    template<typename Fn, typename F>
    static Slot fromF(const std::function<F> & fn,
                      Operations ops = FunctionOps<Fn>::ops,
                      JS::JSOperations jsops = JS::getOps(typeid(Fn)))
    {

        Slot result;
        if (!fn) return result;
        result.fntype = STD_FN;
        result.ops = ops;
        result.jsops = jsops;
        result.fn = new std::function<F>(fn);
        return result;
    }
#else // NODEJS_ENABLED
    template<typename Fn, typename F>
    static Slot fromF(const std::function<F> & fn,
                      Operations ops = FunctionOps<Fn>::ops)
    {

        Slot result;
        if (!fn) return result;
        result.fntype = STD_FN;
        result.ops = ops;
        result.fn = new std::function<F>(fn);
        return result;
    }
#endif // NODEJS_ENABLED

    // Initialize from a std::function where the function type and the
    // called type are identical
#if NODEJS_ENABLED
    template<typename Fn, typename F>
    static Slot fromF(F fn,
                      Operations ops = FunctionOps<Fn>::ops,
                      JS::JSOperations jsops = JS::getOps(typeid(Fn)))
    {
        Slot result;
        //if (!fn) return result;
        result.fntype = STD_FN;
        result.ops = ops;
        result.jsops = jsops;
        result.fn = new std::function<Fn>(fn);
        return result;
    }
#else // NODEJS_ENABLED
    template<typename Fn, typename F>
    static Slot fromF(F fn,
                      Operations ops = FunctionOps<Fn>::ops)
    {
        Slot result;
        //if (!fn)< return result;
        result.fntype = STD_FN;
        result.ops = ops;
        result.fn = new std::function<Fn>(fn);
        return result;
    }
#endif // NODEJS_ENABLED
                 
#if NODEJS_ENABLED
    template<typename Fn>
    Slot(const std::function<Fn> & fn,
         Operations ops = FunctionOps<Fn>::ops,
         JS::JSOperations jsops = JS::getOps(typeid(Fn)))
        : fn(new std::function<Fn>(fn)), ops(ops), jsops(jsops), fntype(STD_FN)
    {
        if (!fn) {
            Slot new_me;
            swap(new_me);
        }
    }
#else // NODEJS_ENABLED
    template<typename Fn>
    Slot(const std::function<Fn> & fn,
         Operations ops = FunctionOps<Fn>::ops)
        : fn(new std::function<Fn>(fn)), ops(ops), fntype(STD_FN)
    {
        if (!fn) {
            Slot new_me;
            swap(new_me);
        }
    }
#endif // NODEJS_ENABLED
    
#if NODEJS_ENABLED
    /** Initialize from a Javascript function. */
    Slot(const v8::Handle<v8::Function> & fn);

    /** Initialize from a Javascript value that must be convertible to a function. */
    Slot(const v8::Handle<v8::Value> & fn);
#endif // NODEJS_ENABLED

    ~Slot();

    /** Print a string representation giving information about the slot. */
    std::string print() const;

    /** Is it empty? */
    bool isEmpty() const { return fntype == EMPTY; }

    /** Call from C++, knowing exactly what kinds of parameters there are. */
    template<typename CallAs, typename... Args>
    typename std::function<CallAs>::result_type
    call(Args... args) const
    {
#if NODEJS_ENABLED
        if (!inJsContext())
            throw ML::Exception("callback outside JS context");
#endif // NODEJS_ENABLED
        return as<CallAs>()(args...);
    }

#if NODEJS_ENABLED
    /** Call from Javascript, unpacking the parameters as we go and wrapping
        up the return value.
    */
    v8::Handle<v8::Value> call(const v8::Arguments & args) const;
    v8::Handle<v8::Value> call(const v8::Handle<v8::Object> & This,
                               int argc, v8::Handle<v8::Value> argv[]) const;

    /** Convert directly into a JS function.  NOTE: this is slow; due to
        v8 limitations we need to actually parse a JS file each time.

        If it was already a JS function, then that function is simply
        returned.
    */
    v8::Local<v8::Function> toJsFunction() const;
#endif // NODEJS_ENABLED

    /** Return the type info node of the function type.  Will return the
        typeinfo node of the v8 function if it's a JS value.
    */
    const std::type_info & type() const;
    
    /** Return a std function object that will synchronously call the
        given callback.
    */
#if NODEJS_ENABLED
    template<typename Fn>
    std::function<Fn> as(JS::JSOperations jsops = 0)
        const
    {
        switch (fntype) {
        case EMPTY:
            return std::function<Fn>();
            //throw ML::Exception("can't convert empty notification");
        case STD_FN:
            if (typeid(Fn) != type())
                throw ML::Exception("couldn't convert function of type "
                                    + ML::demangle(type()) + " to type "
                                    + ML::type_name<Fn>());

            ExcAssert(fn != nullptr);
            return *(std::function<Fn> *) fn;

        case JS: {
            if (!inJsContext())
                throw ML::Exception("callback outside JS context");

            if (!jsops)
                jsops = JS::getOps(typeid(Fn));

            JS::JSAsBoost op = (JS::JSAsBoost)jsops;

            std::function<Fn> result;
            v8::Handle<v8::Object> * This = 0;

            op(1, *jsfn, *This, result);

            return result;
        }
        default:
            throw ML::Exception("unknown operation type");
        }
    }
#else // NODEJS_ENABLED
    template<typename Fn>
    std::function<Fn> as()
        const
    {
        switch (fntype) {
        case EMPTY:
            return std::function<Fn>();
            //throw ML::Exception("can't convert empty notification");
        case STD_FN:
            if (typeid(Fn) != type())
                throw ML::Exception("couldn't convert function of type "
                                    + ML::demangle(type()) + " to type "
                                    + ML::type_name<Fn>());

            ExcAssert(fn != nullptr);
            return *(std::function<Fn> *) fn;

        default:
            throw ML::Exception("unknown operation type");
        }
    }
#endif // NODEJS_ENABLED

    /** Implicit conversion to std::function<x> */
    template<typename Fn>
    operator std::function<Fn> () const
    {
        return as<Fn>();
    }

private:
    union {
        // std::function contents
        struct {
            void * fn;
            Operations ops;
#if NODEJS_ENABLED
            JS::JSOperations jsops;
#endif // NODEJS_ENABLED
        };

#if NODEJS_ENABLED
        // JS function contents
        struct {
            v8::Persistent<v8::Function> * jsfn;
        };
#endif // NODEJS_ENABLED
    };

    /// The kind of object that this slot refers to
    enum FnType {
        EMPTY,   ///< No function there
        STD_FN,  ///< Wrapping a std::function
        JS       ///< Wrapping a JS function
    };

    FnType fntype;  // TODO: find a way to do this without a member

    /** Free all memory associated with this */
    void free();
};

#if NODEJS_ENABLED
template<typename Fn, typename F>
Slot slot(const F & fn,
          Operations ops = FunctionOps<Fn>::ops,
          JS::JSOperations jsops = JS::getOps(typeid(Fn)))
{
    return Slot::fromF<Fn>(std::function<Fn>(fn), ops, jsops);
}
#else // NODEJS_ENABLED
template<typename Fn, typename F>
Slot slot(const F & fn,
          Operations ops = FunctionOps<Fn>::ops)
{
    return Slot::fromF<Fn>(std::function<Fn>(fn), ops);
}
#endif // NODEJS_ENABLED

template<typename Fn>
struct SlotT : public Slot {

    SlotT()
    {
    }

    SlotT(const Slot & slot)
        : Slot(slot)
    {
    }

#if NODEJS_ENABLED
    SlotT(const std::function<Fn> & fn,
         Operations ops = FunctionOps<Fn>::ops,
         JS::JSOperations jsops = JS::getOps(typeid(Fn)))
        : Slot(fn, ops, jsops)
    {
    }
    
    /** Initialize from a Javascript function. */
    SlotT(const v8::Handle<v8::Function> & fn)
        : Slot(fn)
    {
    }

    /** Initialize from a Javascript value that must be convertible to a
        function. */
    SlotT(const v8::Handle<v8::Value> & fn)
        : Slot(fn)
    {
    }
#else // NODEJS_ENABLED
    SlotT(const std::function<Fn> & fn,
         Operations ops = FunctionOps<Fn>::ops)
        : Slot(fn, ops)
    {
    }
#endif // NODEJS_ENABLED

    /** Print a string representation giving information about the slot. */
    std::string print() const;

    /** Call from C++, knowing exactly what kinds of parameters there are. */
    template<typename... Args>
    typename std::function<Fn>::result_type
    call(Args... args) const
    {
        return toStd()(args...);
    }

    template<typename... Args>
    typename std::function<Fn>::result_type
    operator () (Args... args) const
    {
        return call(args...);
    }

    using Slot::call;

    /** Return a std function object that will synchronously call the
        given callback.
    */
#if NODEJS_ENABLED
    std::function<Fn> toStd(JS::JSOperations jsops = 0)
        const
    {
        return Slot::as<Fn>(jsops);
    }
#else // NODEJS_ENABLED
    std::function<Fn> toStd()
        const
    {
        return Slot::as<Fn>();
    }
#endif // NODEJS_ENABLED

    /** Implicit conversion to std::function<x> */
    operator std::function<Fn> () const
    {
        return as<Fn>();
    }
};

#if NODEJS_ENABLED
namespace JS {

struct JSValue;

Slot from_js(const JSValue &, Slot * = 0);

template<typename Fn>
SlotT<Fn> from_js(const JSValue & val, SlotT<Fn> * = 0)
{
    return SlotT<Fn>(from_js(val, (Slot *)0));
}

template<typename Fn>
std::function<Fn> from_js(const JSValue & val, std::function<Fn> * = 0)
{
    return SlotT<Fn>(from_js(val, (Slot *)0)).toStd();
}

template<typename Fn>
std::function<Fn> from_js_ref(const JSValue & val, std::function<Fn> * = 0)
{
    return SlotT<Fn>(from_js(val, (Slot *)0)).toStd();
}

template<typename Fn>
std::function<Fn> from_js(const JSValue & val, std::function<Fn> * = 0)
{
    return SlotT<Fn>(from_js(val, (Slot *)0)).toStd();
}

template<typename Fn>
std::function<Fn> from_js_ref(const JSValue & val, std::function<Fn> * = 0)
{
    return SlotT<Fn>(from_js(val, (Slot *)0)).toStd();
}

void to_js(JSValue & val, const Slot & slot);

template<typename Fn>
void to_js(JSValue & val, const SlotT<Fn> & slot)
{
    return to_js(val, (const Slot &)slot);
}

} // namespace JS
#endif // NODEJS_ENABLED

} // namespace Datacratic
