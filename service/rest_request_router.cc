/* rest_request_router.cc
   Jeremy Barnes, 15 November 2012
   Copyright (c) 2012 Datacratic Inc.  All rights reserved.

*/

#include "rest_request_router.h"
#include "jml/utils/vector_utils.h"
#include "jml/arch/exception_handler.h"
#include "jml/utils/set_utils.h"
#include "jml/utils/environment.h"
#include "jml/utils/file_functions.h"
#include "jml/utils/string_functions.h"

using namespace std;


namespace Datacratic {


/*****************************************************************************/
/* PATH SPEC                                                                 */
/*****************************************************************************/

std::ostream & operator << (std::ostream & stream, const PathSpec & path)
{
    return stream << path.path;
}


/*****************************************************************************/
/* REQUEST FILTER                                                            */
/*****************************************************************************/

std::ostream & operator << (std::ostream & stream, const RequestFilter & filter)
{
    return stream;
}


/*****************************************************************************/
/* REST REQUEST PARSING CONTEXT                                              */
/*****************************************************************************/

std::ostream & operator << (std::ostream & stream,
                            const RestRequestParsingContext & context)
{
    return stream << context.resources << " " << context.remaining;
}


/*****************************************************************************/
/* REST REQUEST ROUTER                                                       */
/*****************************************************************************/

RestRequestRouter::
RestRequestRouter()
    : terminal(false)
{
}

RestRequestRouter::
RestRequestRouter(const OnProcessRequest & processRequest,
                  const std::string & description,
                  bool terminal,
                  const Json::Value & argHelp)
    : rootHandler(processRequest),
      description(description),
      terminal(terminal),
      argHelp(argHelp)
{
}

RestRequestRouter::
~RestRequestRouter()
{
}
    
RestRequestRouter::OnHandleRequest
RestRequestRouter::
requestHandler() const
{
    return std::bind(&RestRequestRouter::handleRequest,
                     this,
                     std::placeholders::_1,
                     std::placeholders::_2);
}

void
RestRequestRouter::
handleRequest(RestConnection & connection,
              const RestRequest & request) const
{
    //JML_TRACE_EXCEPTIONS(false);

    RestRequestParsingContext context(request);
    MatchResult res = processRequest(connection, request, context);
    if (res == MR_NO) {
        connection.sendErrorResponse(404, "unknown resource " + request.verb + " " + request.resource);
    }
}

static std::string getVerbsStr(const std::set<std::string> & verbs)
{
    string verbsStr;
    for (auto v: verbs) {
        if (!verbsStr.empty())
            verbsStr += ",";
        verbsStr += v;
    }
            
    return verbsStr;
}

namespace {

ML::Env_Option<bool, true> TRACE_REST_REQUESTS("TRACE_REST_REQUESTS", false);

} // file scope

RestRequestRouter::
MatchResult
RestRequestRouter::
processRequest(RestConnection & connection,
               const RestRequest & request,
               RestRequestParsingContext & context) const
{
    bool debug = TRACE_REST_REQUESTS;

    if (debug) {
        cerr << "processing request " << request
             << " with context " << context
             << " against route " << description 
             << " with " << subRoutes.size() << " subroutes" << endl;
    }

    if (request.verb == "OPTIONS") {
        Json::Value help;
        std::set<std::string> verbs;

        this->options(verbs, help, request, context);

        RestParams headers = { { "Allow", getVerbsStr(verbs) } };
        
        if (verbs.empty())
            connection.sendHttpResponse(400, "", "", headers);
        else
            connection.sendHttpResponse(200, help.toStyledString(),
                                        "application/json",
                                        headers);
        return MR_YES;
    }

    if (rootHandler && (!terminal || context.remaining.empty()))
        return rootHandler(connection, request, context);

    for (auto & sr: subRoutes) {
        if (debug)
            cerr << "  trying subroute " << sr.router->description << endl;
        try {
            MatchResult mr = sr.process(request, context, connection);
            //cerr << "returned " << mr << endl;
            if (mr == MR_YES || mr == MR_ASYNC || mr == MR_ERROR)
                return mr;
        } catch (const std::exception & exc) {
            connection.sendErrorResponse(500, ML::format("threw exception: %s",
                                                         exc.what()));
        } catch (...) {
            connection.sendErrorResponse(500, "unknown exception");
        }
    }

    return MR_NO;
    //connection.sendErrorResponse(404, "invalid route for "
    //                             + request.resource);
}

void
RestRequestRouter::
options(std::set<std::string> & verbsAccepted,
        Json::Value & help,
        const RestRequest & request,
        RestRequestParsingContext & context) const
{
    for (auto & sr: subRoutes) {
        sr.options(verbsAccepted, help, request, context);
    }
}

bool
RestRequestRouter::Route::
matchPath(const RestRequest & request,
          RestRequestParsingContext & context) const
{
    switch (path.type) {
    case PathSpec::STRING: {
        std::string::size_type pos = context.remaining.find(path.path);
        if (pos == 0) {
            using namespace std;
            //cerr << "context string " << pos << endl;
            context.resources.push_back(path.path);
            context.remaining = string(context.remaining, path.path.size());
            break;
        }
        else return false;
    }
    case PathSpec::REGEX: {
        boost::smatch results;
        bool found
            = boost::regex_search(context.remaining,
                                  results,
                                  path.rex)
            && !results.prefix().matched;  // matches from the start
        
        //cerr << "matching regex " << path.path << " against "
        //     << context.remaining << " with found " << found << endl;
        if (!found)
            return false;
        for (unsigned i = 0;  i < results.size();  ++i)
            context.resources.push_back(results[i]);
        context.remaining = std::string(context.remaining,
                                        results[0].length());
        break;
    }
    case PathSpec::NONE:
    default:
        throw ML::Exception("unknown rest request type");
    }

    return true;
}

RestRequestRouter::MatchResult
RestRequestRouter::Route::
process(const RestRequest & request,
        RestRequestParsingContext & context,
        RestConnection & connection) const
{
    using namespace std;

    bool debug = false;

    if (debug) {
        cerr << "verb = " << request.verb << " filter.verbs = " << filter.verbs
             << endl;
    }
    if (!filter.verbs.empty()
        && !filter.verbs.count(request.verb))
        return MR_NO;

    // Check that the parameter filters match
    for (auto & f: filter.filters) {
        bool matched = false;
        for (auto & p: request.params) {
            if (p.first == f.param && p.second == f.value) {
                matched = true;
                break;
            }
            if (matched) break;
        }
        if (!matched)
            return MR_NO;
    }

    // At the end, make sure we put the context back to how it was
    RestRequestParsingContext::StateGuard guard(&context);

    if (!matchPath(request, context))
        return MR_NO;

    if (extractObject)
        extractObject(connection, request, context);

    if (connection.responseSent())
        return MR_YES;

    return router->processRequest(connection, request, context);
}

void
RestRequestRouter::Route::
options(std::set<std::string> & verbsAccepted,
        Json::Value & help,
        const RestRequest & request,
        RestRequestParsingContext & context) const
{
    RestRequestParsingContext::StateGuard guard(&context);

    if (!matchPath(request, context))
        return;

    if (context.remaining.empty()) {
        verbsAccepted.insert(filter.verbs.begin(), filter.verbs.end());

        string path = "";//this->path.getPathDesc();
        Json::Value & sri = help[path + getVerbsStr(filter.verbs)];
        this->path.getHelp(sri);
        filter.getHelp(sri);
        router->getHelp(help, path, filter.verbs);
    }
    router->options(verbsAccepted, help, request, context);
}

void
RestRequestRouter::
addRoute(PathSpec path, RequestFilter filter,
         const std::shared_ptr<RestRequestRouter> & handler,
         ExtractObject extractObject)
{
    if (rootHandler)
        throw ML::Exception("can't add a sub-route to a terminal route");

    Route route;
    route.path = path;
    route.filter = filter;
    route.router = handler;
    route.extractObject = extractObject;

    subRoutes.emplace_back(std::move(route));
}

void
RestRequestRouter::
addRoute(PathSpec path, RequestFilter filter,
         const std::string & description,
         const OnProcessRequest & cb,
         const Json::Value & argHelp,
         ExtractObject extractObject)
{
    addRoute(path, filter,
             std::make_shared<RestRequestRouter>(cb, description, true, argHelp),
             extractObject);
}

void
RestRequestRouter::
addHelpRoute(PathSpec path, RequestFilter filter)
{
    OnProcessRequest helpRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context)
        {
            if (request.params.hasValue("autodoc")) {
                Json::Value help;
                getAutodocHelp(help, "", set<string>());
                connection.sendResponse(200, help);
            } else {
                Json::Value help;
                getHelp(help, "", set<string>());
                connection.sendResponse(200, help);
            }

            return MR_YES;
        };

    addRoute(path, filter, "Get help on the available API commands",
             helpRoute, Json::Value());
}


void
RestRequestRouter::
addAutodocRoute(PathSpec autodocPath, PathSpec helpPath,
                const string & autodocFilesPath)
{
    string autodocPathStr = autodocPath.getPathDesc();
    OnProcessRequest rootRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {
        connection.sendRedirect(302, autodocPathStr + "/index.html");
        return RestRequestRouter::MR_YES;
    };

    addRoute(autodocPathStr, "GET", "Main autodoc page",
             rootRoute, Json::Value());
    addRoute(autodocPathStr + "/", "GET", "Main autodoc page",
             rootRoute, Json::Value());

    OnProcessRequest autodocRoute
        = [=] (RestConnection & connection,
               const RestRequest & request,
               const RestRequestParsingContext & context) {

        string path = context.resources.back();

        if (path.find("..") != string::npos) {
            throw ML::Exception("not dealing with path with .. in it");
        }

        if (path.find(autodocPathStr) != 0) {
            throw ML::Exception("not serving file not under %",
                                autodocPathStr.c_str());
        }

        string filename = path.substr(autodocPathStr.size());
        if (filename[0] == '/') {
            filename = filename.substr(1);
        }
        if (filename == "autodoc") {
            connection.sendRedirect(302, helpPath.getPathDesc() + "?autodoc");
            return RestRequestRouter::MR_YES;
        }

        ML::File_Read_Buffer buf(autodocFilesPath + "/" + filename);

        string mimeType = "text/plain";
        if (filename.find(".html") != string::npos) {
            mimeType = "text/html";
        }
        else if (filename.find(".js") != string::npos) {
            mimeType = "application/javascript";
        }
        else if (filename.find(".css") != string::npos) {
            mimeType = "text/css";
        }

        string result(buf.start(), buf.end());
        connection.sendResponse(200, result,  mimeType);
        return RestRequestRouter::MR_YES;
    };

    addRoute(Rx(autodocPathStr + "/.*", "<resource>"), "GET",
            "Static content", autodocRoute, Json::Value());
}

void
RestRequestRouter::
getHelp(Json::Value & result, const std::string & currentPath,
        const std::set<std::string> & verbs) const
{
    Json::Value & v = result[(currentPath.empty() ? "" : currentPath + " ")
                             + getVerbsStr(verbs)];

    v["description"] = description;
    if (!argHelp.isNull())
        v["arguments"] = argHelp;
    
    for (unsigned i = 0;  i < subRoutes.size();  ++i) {
        string path = currentPath + subRoutes[i].path.getPathDesc();
        Json::Value & sri = result[(path.empty() ? "" : path + " ")
                                   + getVerbsStr(subRoutes[i].filter.verbs)];
        subRoutes[i].path.getHelp(sri);
        subRoutes[i].filter.getHelp(sri);
        subRoutes[i].router->getHelp(result, path, subRoutes[i].filter.verbs);
    }
}

string
RestRequestRouter::
typeFromCppType(const string & cppType) const {
    if (cppType == "bool") {
        return "boolean";
    }
    if (cppType == "Json::Value") {
        return "object";
    }
    if (cppType == "Datacratic::Date") {
        return "ISO 8601 date-time";
    }

    shared_ptr<const ValueDescription> vd = ValueDescription::get(cppType);
    cerr << "unknown cppType: " << cppType << ", of kind: " << vd->kind << endl;
    return "unknown";
}

string
RestRequestRouter::
typeFromValueKind(const ValueKind & kind) const {
    if (kind == ValueKind::INTEGER) {
        return "integer";
    }
    if (kind == ValueKind::BOOLEAN) {
        return "boolean";
    }
    if (kind == ValueKind::STRING || kind == ValueKind::ENUM
            || kind == ValueKind::LINK) {
        return "string";
    }
    if (kind == ValueKind::FLOAT) {
        return "float";
    }
    if (kind == ValueKind::ARRAY) {
        return "array";
    }
    cerr << "uncovered conversion case for kind: " << kind << endl;
    return "object";
}


void
RestRequestRouter::
addValueDescriptionToProperties(const ValueDescription * vd,
                                Json::Value & properties, int recur) const
{
    if (recur > 2) {
        cerr << "WARNING: Too many recursions" << endl;
        return;
    }
    using namespace Json;
    auto onField = [this, &properties, recur, vd] (const ValueDescription::FieldDescription & fd) {
        Value tmpObj;
        tmpObj["type"] = typeFromValueKind(fd.description->kind);
        tmpObj["description"] = fd.comment;
        if (fd.description->kind == ValueKind::ARRAY) {
            const ValueDescription * subVdPtr = &(fd.description->contained());
            if (subVdPtr->kind == ValueKind::STRUCTURE) {
                if (vd == subVdPtr) {
                    tmpObj["items"]["type"] = "object (recursive)";
                    tmpObj["items"]["properties"] = objectValue;
                }
                else {
                    Value itemProperties;
                    addValueDescriptionToProperties(subVdPtr, itemProperties, recur + 1);
                    tmpObj["items"]["items"]["properties"] = itemProperties;
                }
            }
            else {
                if (subVdPtr->kind == ValueKind::ARRAY) {
                    // unsupported "pair" type
                    tmpObj["items"]["type"] = "object";
                }
                else {
                    tmpObj["items"]["type"] =
                        typeFromValueKind(subVdPtr->kind);
                }
            }
        }
        else if (fd.description->kind == ValueKind::STRUCTURE) {
            Value itemProperties;
            addValueDescriptionToProperties(fd.description.get(), itemProperties, recur + 1);
            tmpObj["items"]["properties"] = itemProperties;
        }
        properties[fd.fieldName] = tmpObj;
    };
    vd->forEachField(nullptr, onField);
}


void
RestRequestRouter::
addJsonParamsToProperties(const Json::Value & params,
                          Json::Value & properties) const
{
    using namespace Json;
    for (ValueIterator paramsIt = params.begin();
            paramsIt != params.end();
            paramsIt++) {
        string cppType = (*paramsIt)["cppType"].asString();
        shared_ptr<const ValueDescription> vd = ValueDescription::get(cppType);
        if (vd->kind == ValueKind::STRUCTURE) {
            addValueDescriptionToProperties(vd.get(), properties);
        }
        else {
            Value tmpObj;
            tmpObj["type"] = typeFromCppType(cppType);
            tmpObj["description"] = (*paramsIt)["description"].asString();
            properties[(*paramsIt)["name"].asString()] = tmpObj;
        }
    }
}

void
RestRequestRouter::
getAutodocHelp(Json::Value & result, const std::string & currentPath,
               const std::set<std::string> & verbs) const
{
    using namespace Json;
    Value tmpResult;
    getHelp(tmpResult, "", set<string>());
    result["routes"]   = arrayValue;
    result["literate"] = arrayValue;
    result["config"]   = objectValue;
    for (ValueIterator it = tmpResult.begin() ; it != tmpResult.end() ; it++) {
        string key = it.key().asString();
        vector<string> parts = ML::split(it.key().asString());
        int size = parts.size();
        if (size == 0) {
            // the empty key contains the description
            continue;
        }
        if (size == 1) {
            // useless route
            continue;
        }
        ExcAssert(size == 2);

        vector<string> verbs = ML::split(parts[1], ',');
        for (const string & verb: verbs) {
            Value curr = arrayValue;
            curr.append(verb + " " + parts[0]);
            Value subObj;
            subObj["out"] = objectValue;
            subObj["out"]["required"] = arrayValue;
            subObj["out"]["type"] = "object";
            subObj["out"]["properties"] = objectValue;
            subObj["required_role"] = nullValue;
            subObj["docstring"] = (*it)["description"].asString();
            subObj["in"] = nullValue;
            subObj["in"]["required"] = arrayValue;
            subObj["in"]["type"] = "object";
            subObj["in"]["properties"] = objectValue;
            if ((*it).isMember("arguments") && (*it)["arguments"].isMember("jsonParams")) {
                addJsonParamsToProperties((*it)["arguments"]["jsonParams"],
                                          subObj["in"]["properties"]);
#if 0
                if ((*it)["arguments"]["jsonParams"].size() == 1) {
                    //possibly a struct
                }
                else {
                    for (ValueIterator paramsIt = (*it)["arguments"]["jsonParams"].begin();
                            paramsIt != (*it)["arguments"]["jsonParams"].end() ;
                            paramsIt++) {
                        Value tmpObj;
                        tmpObj["type"] = typeFromCppType((*paramsIt)["cppType"].asString());
                        tmpObj["description"] = (*paramsIt)["description"].asString();
                        subObj["in"]["properties"][(*paramsIt)["name"].asString()] = tmpObj;
                    }
                }
#endif
            }
            curr.append(subObj);
            result["routes"].append(curr);
        };
    }
}

RestRequestRouter &
RestRequestRouter::
addSubRouter(PathSpec path, const std::string & description, ExtractObject extractObject,
             std::shared_ptr<RestRequestRouter> subRouter)
{
    // TODO: check it doesn't exist
    Route route;
    route.path = path;
    if (subRouter)
        route.router = subRouter;
    else route.router.reset(new RestRequestRouter());

    route.router->description = description;
    route.extractObject = extractObject;

    subRoutes.push_back(route);
    return *route.router;
}


} // namespace Datacratic
