/* service_base.cc
   Jeremy Barnes, 29 May 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   Service base.
*/

#include "service_base.h"
#include <iostream>
#include "soa/service/carbon_connector.h"
#include "zookeeper_configuration_service.h"
#include "jml/arch/demangle.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/environment.h"
#include "jml/utils/file_functions.h"
#include <unistd.h>
#include <sys/utsname.h>
#include <boost/make_shared.hpp>
#include "zmq.hpp"
#include "soa/jsoncpp/reader.h"
#include "soa/jsoncpp/value.h"
#include <fstream>
#include <sys/utsname.h>

using namespace std;

extern const char * __progname;

namespace Datacratic {


/*****************************************************************************/
/* NULL EVENT SERVICE                                                        */
/*****************************************************************************/

NullEventService::
NullEventService()
    : stats(new MultiAggregator())
{
}

NullEventService::
~NullEventService()
{
}

void
NullEventService::
onEvent(const std::string & name,
        const char * event,
        EventType type,
        float value)
{
    stats->record(name + "." + event, type, value);
}

void
NullEventService::
dump(std::ostream & stream) const
{
    stats->dumpSync(stream);
}


/*****************************************************************************/
/* CARBON EVENT SERVICE                                                      */
/*****************************************************************************/

CarbonEventService::
CarbonEventService(std::shared_ptr<CarbonConnector> conn) :
    connector(conn)
{
}

CarbonEventService::
CarbonEventService(const std::string & carbonAddress,
                   const std::string & prefix)
    : connector(new CarbonConnector(carbonAddress, prefix))
{
}

CarbonEventService::
CarbonEventService(const std::vector<std::string> & carbonAddresses,
                   const std::string & prefix)
    : connector(new CarbonConnector(carbonAddresses, prefix))
{
}

void
CarbonEventService::
onEvent(const std::string & name,
        const char * event,
        EventType type,
        float value)
{
    if (name.empty()) {
        connector->record(event, type, value);
    }
    else {
        size_t l = strlen(event);
        string s;
        s.reserve(name.length() + l + 2);
        s.append(name);
        s.push_back('.');
        s.append(event, event + l);
        connector->record(s, type, value);
    }
}


/*****************************************************************************/
/* CONFIGURATION SERVICE                                                     */
/*****************************************************************************/

void
ConfigurationService::
dump(std::ostream & stream) const
{
    auto onValue = [&] (string key, const Json::Value & val)
        {
            stream << key << " --> " << val.toString();
            return true;
        };

    forEachEntry(onValue, "");
}

Json::Value
ConfigurationService::
jsonDump() const
{
    Json::Value allEntries;

    auto onValue = [&] (string key, const Json::Value & val)
        {
            allEntries[key] = val;
            return true;
        };
    forEachEntry(onValue, "");

    return allEntries;
}

std::pair<std::string, std::string>
ConfigurationService::
splitPath(const std::string & path)
{
    string::size_type pos = path.find('/');

    string root(path, 0, pos);
    string leaf;
    if (pos != string::npos)
        leaf = string(path, pos + 1);

    return make_pair(root, leaf);
}

/*****************************************************************************/
/* INTERNAL CONFIGURATION SERVICE                                            */
/*****************************************************************************/

Json::Value
InternalConfigurationService::
getJson(const std::string & key, Watch watch)
{
    Guard guard(lock);

    Entry * entry = getNode(root, key);
    if (!entry)
        return Json::Value();

    entry->watch = watch;
    return entry->value;
}
    
void
InternalConfigurationService::
set(const std::string & key,
    const Json::Value & value)
{
    Guard guard(lock);
    Entry & node = createNode(root, key);
    node.hasValue = true;
    node.value = value;
    if (node.watch) {
        node.watch.trigger(key, VALUE_CHANGED);
        node.watch = Watch();
    }
}

std::string
InternalConfigurationService::
setUnique(const std::string & key_,
          const Json::Value & value)
{
    Guard guard(lock);

    int r = 0;

    for (;; r = random()) {
        string key = key_;
        if (r != 0)
            key += ":" + to_string(r);
        
        Entry & node = createNode(root, key);
        if (node.hasValue)
            continue;
        
        node.hasValue = true;
        node.value = value;
        
        return key;
    }
}

std::vector<std::string>
InternalConfigurationService::
getChildren(const std::string & key,
            Watch watch)
{
    Guard guard(lock);

    auto node = getNode(root, key);
    vector<string> result;
    if (node) {
        for (auto & ch: node->children)
            result.push_back(ch.first);
        node->watch = watch;
    }
    return result;
}

bool
InternalConfigurationService::
forEachEntry(const OnEntry & onEntry,
             const std::string & startPrefix) const
{
    Guard guard(lock);

    std::function<bool (const Entry *, string)> doNode
        = [&] (const Entry * node, string prefix)
        {
            if (!node)
                return true;
            
            if (node->hasValue)
                if (!onEntry(prefix, node->value))
                    return false;

            for (auto ch: node->children)
                if (!doNode(ch.second.get(), prefix + "/" + ch.first))
                    return false;
            return true;
        };

    return doNode(getNode(root, startPrefix), startPrefix);
}

InternalConfigurationService::Entry &
InternalConfigurationService::
createNode(Entry & node, const std::string & key)
{
    if (key == "")
        return node;

    string root, leaf;
    std::tie(root, leaf) = splitPath(key);

    bool existed = node.children.count(root);

    shared_ptr<Entry> & entryPtr = node.children[root];
    if (!entryPtr)
        entryPtr.reset(new Entry());
    else if (entryPtr->watch && !existed) {
        entryPtr->watch.trigger(key, VALUE_CHANGED);
        entryPtr->watch = Watch();
    }

    return createNode(*entryPtr, leaf);
}

const InternalConfigurationService::Entry *
InternalConfigurationService::
getNode(const Entry & node, const std::string & key) const
{
    if (key == "")
        return &node;

    string root, leaf;
    std::tie(root, leaf) = splitPath(key);

    auto it = node.children.find(root);
    if (it == node.children.end())
        return 0;

    ExcAssert(it->second);

    return getNode(*it->second, leaf);
}

InternalConfigurationService::Entry *
InternalConfigurationService::
getNode(Entry & node, const std::string & key)
{
    if (key == "")
        return &node;

    string root, leaf;
    std::tie(root, leaf) = splitPath(key);

    auto it = node.children.find(root);
    if (it == node.children.end())
        return 0;

    ExcAssert(it->second);

    return getNode(*it->second, leaf);
}

void
InternalConfigurationService::
removePath(const std::string & key)
{
    string path, leaf;
    std::tie(path, leaf) = splitPath(key);

    Guard guard(lock);
    Entry * parent = getNode(root, path);
    if (!parent)
        return;

    // TODO: listeners for onChange, both here and children

    parent->children.erase(leaf);
}


/*****************************************************************************/
/* SERVICE PROXIES                                                           */
/*****************************************************************************/

namespace {

std::string bootstrapConfigPath()
{
    ML::Env_Option<string> env("RTBKIT_BOOTSTRAP", "");
    if (!env.get().empty()) return env.get();

    const string cwdPath = "./bootstrap.json";
    if (ML::fileExists(cwdPath)) return cwdPath;

    return "";
}

} // namespace anonymous

ServiceProxies::
ServiceProxies()
    : events(new NullEventService()),
      config(new InternalConfigurationService()),
      ports(new DefaultPortRangeService()),
      zmqContext(new zmq::context_t(1 /* num worker threads */))
{
    bootstrap(bootstrapConfigPath());
}

void
ServiceProxies::
logToCarbon(const std::string & carbonConnection,
            const std::string & prefix)
{
    events.reset(new CarbonEventService(carbonConnection, prefix));
}

void
ServiceProxies::
logToCarbon(const std::vector<std::string> & carbonConnections,
            const std::string & prefix)
{
    events.reset(new CarbonEventService(carbonConnections, prefix));
}

void
ServiceProxies::
logToCarbon(std::shared_ptr<CarbonConnector> conn)
{
    events.reset(new CarbonEventService(conn));
}

void
ServiceProxies::
useZookeeper(std::string hostname,
             std::string prefix)
{
    if (prefix == "CWD") {
        char buf[1024];
        if (!getcwd(buf, 1024))
            throw ML::Exception(errno, "getcwd");
        string cwd = buf;

        utsname name;
        if (uname(&name))
            throw ML::Exception(errno, "uname");
        string node = name.nodename;
        
        prefix = "/dev/" + node + cwd + "_" + __progname + "/";
    }

    config.reset(new ZookeeperConfigurationService(hostname, prefix));
}

void
ServiceProxies::
usePortRanges(const std::string& path)
{
    ports.reset(new JsonPortRangeService(path));
}

void
ServiceProxies::
usePortRanges(const Json::Value& config)
{
    ports.reset(new JsonPortRangeService(config));
}

std::vector<std::string>
ServiceProxies::getServiceClassInstances(std::string const & name,
                                         std::string const & protocol)
{
    std::vector<std::string> result;
    if(config) {
        std::string root = "serviceClass/" + name;
        vector<string> children = config->getChildren(root);
        for(auto & item : children) {
            Json::Value json = config->getJson(root + "/" + item);
            auto items = getEndpointInstances(json["servicePath"].asString(), protocol);
            result.insert(result.begin(), items.begin(), items.end());
        }
    }

    return result;
}

std::vector<std::string>
ServiceProxies::getEndpointInstances(std::string const & name,
                                     std::string const & protocol)
{
    std::vector<std::string> result;
    if(config) {
        std::string path = name + "/" + protocol;
        vector<string> children = config->getChildren(path);
        for(auto & item : children) {
            Json::Value json = config->getJson(path + "/" + item);
            for(auto & entry: json) {
                std::string key;
                if(protocol == "http") key = "httpUri";
                if(protocol == "zeromq") key = "zmqConnectUri";

                if(key.empty() || !entry.isMember(key))
                    continue;

                auto hs = entry["transports"][0]["hostScope"];
                if(!hs)
                    continue;

                string hostScope = hs.asString();
                if(hs != "*") {
                    utsname name;
                    if(uname(&name))
                        throw ML::Exception(errno, "uname");
                    if(hostScope != name.nodename)
                        continue;
                }

                result.push_back(entry[key].asString());
            }
        }
    }

    return result;
}

void
ServiceProxies::
bootstrap(const std::string& path)
{
    if (path.empty()) return;
    ExcCheck(ML::fileExists(path), path + " doesn't exist");

    ifstream stream(path);
    ExcCheckErrno(stream, "Unable to open the Json port range file.");

    string file;
    while(stream) {
        string line;
        getline(stream, line);
        file += line + "\n";
    }

    bootstrap(Json::parse(file));
}

void
ServiceProxies::
bootstrap(const Json::Value& config)
{
    string install = config["installation"].asString();
    ExcCheck(!install.empty(), "installation is not specified in bootstrap.json");

    string node = config["node-name"].asString();
    if (node.empty()) {
        struct utsname s;
        int ret = uname(&s);
        ExcCheckErrno(!ret, "Unable to call uname");

        node = string(s.nodename);
    }

    if (config.isMember("carbon-uri")) {
        const Json::Value& entry = config["carbon-uri"];
        vector<string> uris;

        if (entry.isArray()) {
            for (size_t j = 0; j < entry.size(); ++j)
                uris.push_back(entry[j].asString());
        }
        else uris.push_back(entry.asString());

        logToCarbon(uris, install + "." + node);
    }


    if (config.isMember("zookeeper-uri"))
        useZookeeper(config["zookeeper-uri"].asString(), install);

    if (config.isMember("portRanges"))
        usePortRanges(config["portRanges"]);
}

/*****************************************************************************/
/* EVENT RECORDER                                                            */
/*****************************************************************************/

EventRecorder::
EventRecorder(const std::string & eventPrefix,
              const std::shared_ptr<EventService> & events)
    : eventPrefix_(eventPrefix),
      events_(events)
{
}

EventRecorder::
EventRecorder(const std::string & eventPrefix,
              const std::shared_ptr<ServiceProxies> & services)
    : eventPrefix_(eventPrefix),
      services_(services)
{
}

void
EventRecorder::
recordEventFmt(EventType type,
               float value,
               const char * fmt, ...) const
{
    if (!events_ && (!services_ || !services_->events))  return;
        
    char buf[2048];
        
    va_list ap;
    va_start(ap, fmt);
    try {
        int res = vsnprintf(buf, 2048, fmt, ap);
        if (res < 0)
            throw ML::Exception("unable to record hit with fmt");
        if (res >= 2048)
            throw ML::Exception("key is too long");
            
        recordEvent(buf, type, value);
        va_end(ap);
        return;
    }
    catch (...) {
        va_end(ap);
        throw;
    }
}



/*****************************************************************************/
/* SERVICE BASE                                                              */
/*****************************************************************************/

ServiceBase::
ServiceBase(const std::string & serviceName,
            std::shared_ptr<ServiceProxies> services)
    : EventRecorder(serviceName, services), 
      services_(services), serviceName_(serviceName), parent_(0)
{
    if (!services_)
        setServices(std::make_shared<ServiceProxies>());

    // Clear out any old entries
    getServices()->config->removePath(serviceName);
}

ServiceBase::
ServiceBase(const std::string & subServiceName,
            ServiceBase & parent)
    : EventRecorder(parent.serviceName() + "." + subServiceName,
                    parent.getServices()),
      services_(parent.getServices()),
      serviceName_(parent.serviceName() + "." + subServiceName),
      parent_(&parent)
{
    // Clear out any old entries
    getServices()->config->removePath(serviceName());
}

ServiceBase::
~ServiceBase()
{
}

void
ServiceBase::
registerServiceProvider(const std::string & name,
                        const std::vector<std::string> & serviceClasses)
{
    for (auto cl: serviceClasses) {
        Json::Value json;
        json["serviceName"] = name;
        json["servicePath"] = name;
        services_->config->setUnique("serviceClass/" + cl + "/" + name, json);
    }
}

void
ServiceBase::
unregisterServiceProvider(const std::string & name,
                          const std::vector<std::string> & serviceClasses)
{
    for (auto cl: serviceClasses) {
        services_->config->removePath("serviceClass/" + cl + "/" + name);
    }
}

Json::Value
ServiceBase::
getServiceStatus() const
{
    Json::Value result;
    result["type"] = ML::type_name(*this);
    result["status"] = "Type has not implemented getStatus";
    addChildServiceStatus(result);
    return result;
}

void
ServiceBase::
addChildServiceStatus(Json::Value & result) const
{
    
}

} // namespace Datacratic
