#include "jml/arch/exception_hook.h"

#include "js_utils.h"

using namespace Datacratic::JS;


namespace {

struct Install_Handler {
    Install_Handler()
    {
        ML::exception_tracer = nodeExceptionTracer;
    }

    ~Install_Handler()
    {
        if (ML::exception_tracer == nodeExceptionTracer)
            ML::exception_tracer = 0;
    }
} install_handler;

} // file scope
