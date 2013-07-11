/* zookeeper.cc
   Jeremy Barnes, 17 August 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

*/

#include "soa/service/zookeeper.h"
#include "jml/arch/timers.h"

using namespace std;

namespace Datacratic {

namespace {

struct Init {
    Init()
    {
        zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    }
} init;

void zk_callback(zhandle_t * ah, int type, int state, const char * path, void * user) {
    auto cb = reinterpret_cast<ZookeeperConnection::Callback *>(user);
    if(cb) {
        cb->unlink();
        cb->call(type);
    }
}

} // file scope

/*****************************************************************************/
/* ZOOKEEPER CONNECTION                                                      */
/*****************************************************************************/

std::mutex ZookeeperConnection::lock;

ZookeeperConnection::
ZookeeperConnection()
    : recvTimeout(1000 /* one second */),
      handle(0)
{
}
    
std::string
ZookeeperConnection::
printEvent(int eventType)
{
    if (eventType == ZOO_CREATED_EVENT)
        return "CREATED";
    else if (eventType == ZOO_DELETED_EVENT)
        return "DELETED";
    else if (eventType == ZOO_CHANGED_EVENT)
        return "CHANGED";
    else if (eventType == ZOO_CHILD_EVENT)
        return "CHILD";
    else if (eventType == ZOO_SESSION_EVENT)
        return "SESSION";
    else if (eventType == ZOO_NOTWATCHING_EVENT)
        return "NOTWATCHING";
    else
        return ML::format("UNKNOWN(%d)", eventType);
}

std::string
ZookeeperConnection::
printState(int state)
{
    if (state == ZOO_EXPIRED_SESSION_STATE)
        return "EXPIRED";
    else if (state == ZOO_AUTH_FAILED_STATE)
        return "AUTH_FAILED";
    else if (state == ZOO_CONNECTING_STATE)
        return "CONNECTING";
    else if (state == ZOO_CONNECTED_STATE)
        return "CONNECTED";
    else if (state == ZOO_ASSOCIATING_STATE)
        return "ASSOCIATING";
    else 
        return ML::format("UNKNOWN(%d)", state);
}

void
ZookeeperConnection::
connect(const std::string & host,
        double timeoutInSeconds)
{
    std::unique_lock<std::mutex> lk(connectMutex);
    if (handle)
        throw ML::Exception("can't connect; handle already exists");

    this->host = host;

    int wait = timeoutInSeconds * 1000;
    int timeout = wait;
    int times = 3;

    for(int i = 0; i != times; ++i) {
        handle = zookeeper_init(host.c_str(), eventHandlerFn, recvTimeout, 0, this, 0);

        if (!handle)
            throw ML::Exception(errno, "failed to initialize ZooKeeper at " + host);

        if (cv.wait_for(lk, std::chrono::milliseconds(timeout)) == 
            std::cv_status::no_timeout) {
            return;
        }

        zookeeper_close(handle);
        handle = 0;
        
        int ms = wait + (std::rand() % wait);
        wait *= 2;

        ML::sleep(ms / 1000.0);
    }

    throw ML::Exception("connection to Zookeeper timed out");
}

void
ZookeeperConnection::
reconnect()
{
    if(handle) {
        zookeeper_close(handle);
        handle = 0;
    }

    while(callbacks.next != &callbacks) {
        auto i = callbacks.next;
        i->unlink();
        i->call(ZOO_DELETED_EVENT);
    }

    ML::sleep(1);

    connect(host);

    for(auto & item : ephemerals) {
        createNode(item.path, item.value, true, false, true, true);
    }
}

void
ZookeeperConnection::
close()
{
    using namespace std;
    if (!handle)
        return;
    int res = zookeeper_close(handle);
    handle = 0;
    if (res != ZOK)
        cerr << "warning: closing returned error: " << zerror(res) << endl;

    ephemerals.clear();
}

ZookeeperConnection::CheckResult
ZookeeperConnection::
checkRes(int returnCode, int & retries,
         const char * operation, const char * path)
{
    int maxRetries = 3;

    if (returnCode == ZOK)
        return CR_DONE;

    if (retries < maxRetries
        && is_unrecoverable(handle) != ZOK) {
        reconnect();
        ++retries;
        return CR_RETRY;
    }

    if (retries < maxRetries
        && (returnCode == ZCLOSING
            || returnCode == ZSESSIONMOVED
            || returnCode == ZSESSIONEXPIRED
            || returnCode == ZINVALIDSTATE
            || returnCode == ZOPERATIONTIMEOUT
            || returnCode == ZCONNECTIONLOSS)) {
        reconnect();
        ++retries;
        return CR_RETRY;
    }

    cerr << "zookeeper error on " << operation << ", path "
         << path << ": " << zerror(returnCode) << endl;
    throw ML::Exception("Zookeeper error on %s, path %s: %s",
                        operation, path, zerror(returnCode));
}

void
ZookeeperConnection::
createPath(const std::string & path)
{
    if (path == "/")
        return;
    string::size_type pos = path.rfind('/');
    if (pos == string::npos)
        return;

    string prefix(path, 0, pos);

    int retries = 0;
    for (;;) {
        int res = zoo_create(handle,
                             prefix.c_str(),
                             0, 0,
                             &ZOO_OPEN_ACL_UNSAFE,
                             0, 0, 0);

        if (res == ZNODEEXISTS)
            return;
        
        if (res == ZNONODE) {
            createPath(prefix);
            continue;
        }

        if (checkRes(res, retries, "zoo_create", path.c_str()) == CR_DONE)
            break;
    }
}

void
ZookeeperConnection::
removePath(const std::string & path_)
{
    string path = fixPath(path_);

    std::function<void (const std::string &)> doNode;
    doNode = [&] (const std::string & currentPath)
        {
            vector<string> children
                = getChildren(currentPath,
                              false /* throwIfNodeMissing */);
            
            for (auto child: children)
                if (currentPath[currentPath.size() - 1] == '/')
                    doNode(currentPath + child);
                else
                    doNode(currentPath + "/" + child);
            
            deleteNode(currentPath, false /* throwIfNodeMissing */);
        };

    doNode(path);
}

std::pair<std::string, bool>
ZookeeperConnection::
createNode(const std::string & path,
           const std::string & value,
           bool ephemeral,
           bool sequence,
           bool mustSucceed,
           bool createPath)
{
    //cerr << "createNode for " << path << endl;

    int flags = 0;
    if (ephemeral)
        flags |= ZOO_EPHEMERAL;
    if (sequence)
        flags |= ZOO_SEQUENCE;

    int pathBufLen = path.size() + 256;
    char pathBuf[pathBufLen];

    int retries = 0;
    for (;;) {
        int res = zoo_create(handle,
                             path.c_str(),
                             value.c_str(),
                             value.size(),
                             &ZOO_OPEN_ACL_UNSAFE,
                             flags,
                             pathBuf,
                             pathBufLen);

        if (!mustSucceed && res == ZNODEEXISTS)
            return make_pair(path, false);
        
        if (res == ZNONODE && createPath) {
            //cerr << "createPath for " << path << endl;
            this->createPath(path);
            continue;
        }

        if (checkRes(res, retries, "zoo_create", path.c_str()) == CR_DONE)
            break;
    }

    if(ephemeral) {
        ephemerals.insert(Node(path, value));
    }

    return make_pair<string, bool>(pathBuf, true);
}

bool
ZookeeperConnection::
deleteNode(const std::string & path, bool throwIfNodeMissing)
{
    int retries = 0;
    for (;;) {
        int res = zoo_delete(handle, path.c_str(), -1);

        if (!throwIfNodeMissing && res == ZNONODE)
            return false;
        
        if (checkRes(res, retries, "zoo_delete", path.c_str()) == CR_DONE)
            break;
    }

    ephemerals.erase(path);

    return true;
}

std::string
ZookeeperConnection::
fixPath(const std::string & path)
{
    if (path == "/")
        return path;
    int last = path.size();
    while (last > 0 && path[last - 1] == '/')
        --last;
    return string(path, 0, last);
}

bool
ZookeeperConnection::
nodeExists(const std::string & path_, Callback::Type watcher, void * watcherData)
{
    string path = fixPath(path_);

    int retries = 0;
    for (;;) {

        Callback * cb = getCallback(watcher, path, watcherData);

        int res = zoo_wexists(handle, path.c_str(), zk_callback,
                              cb, 0 /* stat */);
        if (res == ZNONODE)
            return false;
        if (checkRes(res, retries, "zoo_wexists", path.c_str()) == CR_DONE)
            break;
    }

    return true;
}

std::string
ZookeeperConnection::
readNode(const std::string & path_, Callback::Type watcher, void * watcherData)
{
    string path = fixPath(path_);

    char buf[16384];
    int bufLen = 16384;

    int retries = 0;
    for (;;bufLen = 16384) {

        Callback * cb = getCallback(watcher, path, watcherData);

        int res = zoo_wget(handle, path.c_str(), zk_callback, cb,
                           buf, &bufLen, 0 /* stat */);
        if (res == ZNONODE)
            return "";
        if (checkRes(res, retries, "zoo_wget", path.c_str()) == CR_DONE)
            break;
    }

    return string(buf, buf + bufLen);
}

void
ZookeeperConnection::
writeNode(const std::string & path, const std::string & value)
{
    //cerr << "writeNode to " << path << endl;

    int retries = 0;
    for (;;) {
        int res = zoo_set(handle, path.c_str(), value.c_str(),
                          value.size(), -1);
        if (checkRes(res, retries, "zoo_set", path.c_str()) == CR_DONE)
            break;
    }

    auto i = ephemerals.find(path);
    if(ephemerals.end() != i) {
        i->value = value;
    }
}

std::vector<std::string>
ZookeeperConnection::
getChildren(const std::string & path_, bool failIfNodeMissing,
            Callback::Type watcher, void * watcherData)
{
    string path = fixPath(path_);

    //cerr << "getChildren for " << path << ", " << path_ << endl;

    String_vector strings;

    std::vector<std::string> result;

    int retries = 0;
    for (;;) {
        Callback * cb = getCallback(watcher, path, watcherData);

        int res = zoo_wget_children(handle, path.c_str(),
                                    zk_callback, cb,
                                    &strings);

        //cerr << "zoo_wget_children for " << path << " returned "
        //     << res << " and " << strings.count << " strings" << endl;

        if (res == ZNONODE && !failIfNodeMissing) {

            //if (watcher)
            //    cerr << "********* adding wexists handler to " << path << endl;

            // If the node didn't exist then our watch wasn't set... so we
            // set it up here.
            if (watcher) {

                Callback * cb = getCallback(watcher, path, watcherData);

                res = zoo_wexists(handle, path.c_str(), zk_callback, cb, 0);
                //cerr << "wexists handler returned " << res << endl;

                if (res == ZOK) {
                    cerr << "wexists handler: node appeared" << endl;
                    continue;  // node suddenly appeared
                }
                else if (res == ZNONODE)
                    return result;
                if (checkRes(res, retries, "zoo_wexists", path.c_str()) == CR_DONE)
                    return result;
            }

            return result;
        }

        if (checkRes(res, retries, "zoo_get_children", path.c_str()) == CR_DONE)
            break;
    }

    for (unsigned i = 0;  i < strings.count;  ++i)
        result.push_back(strings.data[i]);

    deallocate_String_vector(&strings);

    return result;
        
}

void
ZookeeperConnection::
eventHandlerFn(zhandle_t * handle,
               int event,
               int state,
               const char * path,
               void * context)
{
    ZookeeperConnection * connection = reinterpret_cast<ZookeeperConnection *>(context);

    using namespace std;
    //cerr << "got event " << printEvent(event) << " state " << printState(state) << " on path " << path << endl;

    if(state == ZOO_CONNECTED_STATE) {
        connection->cv.notify_all();
    }
}

} // namespace Datacratic
