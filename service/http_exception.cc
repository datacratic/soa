/** http_exception.cc
    Jeremy Barnes, 2 April 2015

*/

#include "http_exception.h"


using namespace std;


namespace Datacratic {

void rethrowHttpException(int code, const Utf8String & message, Any details)
{
    cerr << "TODO: inject existing message" << endl;
    throw HttpReturnException(code, message, details);
}

void rethrowHttpException(int code, const std::string & message, Any details)
{
    cerr << "TODO: inject existing message" << endl;
    throw HttpReturnException(code, message, details);
}

} // namespace Datacratic
