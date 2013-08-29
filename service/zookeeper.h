/* zookeeper.h                                                     -*- C++ -*-
   Jeremy Barnes, 17 August 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

*/

#pragma once

#include <zookeeper/zookeeper.h>

#include "jml/arch/exception.h"
#include "jml/arch/format.h"
#include "jml/utils/guard.h"

#include <set>
#include <iostream>
#include <vector>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <unordered_map>


namespace Datacratic {

/*****************************************************************************/
/* ZOOKEEPER CONNECTION                                                      */
/*****************************************************************************/

struct ZookeeperConnection {

    typedef std::function<void(int)> Callback; // (type)
    typedef std::function<void(int, const std::string&, void*)> CallbackType; // (type, (deprecated) path, (deprecated) data)

    ZookeeperConnection();
    ~ZookeeperConnection() { close(); }

    static std::string printEvent(int eventType);

    static std::string printState(int state);

    /** Connect synchronously. */
    void connect(const std::string & host,
                 double timeoutInSeconds = 5);

    void reconnect();

    void close();

    enum CheckResult {
        CR_RETRY,  ///< Retry the operation
        CR_DONE    ///< Finish the operation
    };

    /** Check the result of an operation.  Will either return
        RETRY if the operation should be redone, DONE if the
        operation completed, or will throw an exception if there
        was an error.
    */
    CheckResult checkRes(int returnCode, int & retries,
                         const char * operation, const char * path);

    std::pair<std::string, bool>
    createNode(const std::string & path,
               const std::string & value,
               bool ephemeral,
               bool sequence,
               bool mustSucceed = true,
               bool createPath = false);

    /** Delete the given node.  If throwIfNodeMissing is false, then a missing
        node will not be considered an error.  Returns if the node was deleted
        or not, and throws an exception in the case of an error.
    */
    bool deleteNode(const std::string & path, bool throwIfNodeMissing = true);

    /** Create nodes such that the given path exists. */
    void createPath(const std::string & path);

    /** Remove the entire path including all children. */
    void removePath(const std::string & path);

    /** Return if the node exists or not. */
    bool nodeExists(const std::string & path,
                    CallbackType watcher = 0,
                    void * watcherData = 0);

    std::string readNode(const std::string & path,
                         CallbackType watcher = 0,
                         void * watcherData = 0);

    void writeNode(const std::string & path, const std::string & value);

    std::vector<std::string>
    getChildren(const std::string & path,
                bool failIfNodeMissing = true,
                CallbackType watcher = 0,
                void * watcherData = 0);

    static void eventHandlerFn(zhandle_t * handle,
                               int event,
                               int state,
                               const char * path,
                               void * context);

    /** Remove trailing slash so it can be used as a path. */
    static std::string fixPath(const std::string & path);

    std::mutex connectMutex;
    std::condition_variable cv;
    std::string host;
    int recvTimeout;
    clientid_t clientId;
    zhandle_t * handle;

    struct Node {
        Node(std::string const & path) : path(path) {
        }

        Node(std::string const & path, std::string const & value) : path(path), value(value) {
        }

        bool operator<(Node const & other) const {
            return path < other.path;
        }

        std::string path;
        mutable std::string value;
    };

    std::set<Node> ephemerals;

private:
    std::unordered_map<Callback*, std::unique_ptr<Callback>> callbacks;
    Callback* getCallback(CallbackType watch, const std::string& path, void* data);
};

} // namespace Datacratic
