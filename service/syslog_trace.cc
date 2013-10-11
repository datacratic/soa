/* syslog_trace.cc
   Mathieu Stefani, 23 September 2013

   Utility to collect RTBKit traces from syslog
*/

#include <array>

#include <iostream>
#include <string>
#include <sstream>
#include <chrono>

#include "soa/jsoncpp/json.h"
#include "soa/service/nprobe.h"
#include "soa/service/service_base.h"
#include "soa/service/rest_service_endpoint.h"
#include "soa/service/rest_request_router.h"
#include "soa/service/rest_request_binding.h"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "soa/service/service_utils.h"

namespace {

constexpr size_t MaxEntries = 1 << 4;

}

static_assert(!(MaxEntries & 1), "MaxEntries must be 2^M");

using namespace Datacratic;

struct TracingRestEndpoint : public ServiceBase, public RestServiceEndpoint {

    struct TraceEntry {
        struct Context {
            std::string kind;
            int64_t freq;
            std::string uniq;
        } context;

        struct Span {
            int64_t tid;
            int64_t id;
            int64_t parent_id;
            std::string tag;
            int64_t pid;

            std::chrono::nanoseconds start;
            std::chrono::nanoseconds end;
        } span;

        std::string hostname;

        static TraceEntry fromJson(const Json::Value &root) {
            TraceEntry entry;
            try {
                const auto tid = root["tid"].asInt();
                const auto hostname = root["host"].asString();

                const auto id = root["id"].asInt();
                const auto parent_id = root["pid"].asInt();
                const auto kind = root["kind"].asString();
                const auto tag = root["tag"].asString();
                const auto uniq = root["uniq"].asString();
                const int freq = root["freq"].asInt();
                const auto pid = root["kpid"].asInt();

                const auto start = std::chrono::nanoseconds { root["t1"].asInt() };
                const auto end = std::chrono::nanoseconds { root["t2"].asInt() };

                entry.context = { kind, freq, uniq };
                entry.span = { tid, id, parent_id, tag, pid, start, end };
                entry.hostname = hostname;

            } catch (const std::runtime_error &e) {
            }

            return entry;
        }

        std::string print() const {
            std::ostringstream oss;
            oss << "TraceEntry { ";
            oss << " span { " 
                    << "tid = " << span.tid
                    << ", id = " << span.id << ", parent_id = " << span.parent_id
                    << ", tag = " << span.tag << ", pid = " << span.pid << " }"
                << " context { "
                    << "kind = " << context.kind << ", uniq = " << context.uniq
                    << ", freq = " << context.freq << " }"
                << " }";
            return oss.str();
        }
    };

    TracingRestEndpoint(
            const std::shared_ptr<ServiceProxies> &proxies,
            const std::string &name = "tracing.rest-endpoint"
            )
        : ServiceBase(name, proxies), RestServiceEndpoint(getServices()->zmqContext)
        , index { 0 }
        {
            init(getServices()->config, serviceName());
            installRoutes();
        }


    int run(const std::string &fifoPath) {
        int fd = open(fifoPath.c_str(), O_RDONLY);

        if (fd == -1) {
            ::perror("open");
            return 1;
        }

        char c;
        ssize_t bytes { 0 };
        std::string message;
        bool inMessage { false };
        for (;;) {
            if ((bytes = read(fd, &c, 1)) == -1) {
                ::perror("read");
                return 1;
            }

            if (c == '}') {
                message += c;
                if (!handleMessage(message)) 
                    std::cerr << "Failed to handle message: " << message << std::endl;
                message.clear();
                inMessage = false;
            } else if (c == '{') {
                inMessage = true;
            }

            if (inMessage) {
                message += c;
            }
        }
    }

private:
    std::array<TraceEntry, MaxEntries> entries;
    uint64_t index;

    RestRequestRouter restRouter;

    struct StatsEntry {
        StatsEntry(const std::string &tag, const std::vector<TraceEntry> &serie)
           : tag { tag }
           , centile { 0.0 }
           , mean { 0.0 }
           , median { 0.0 }
           , serie_ { serie }
          {  
             compute();
          }

        StatsEntry(StatsEntry &&other) = default;
        StatsEntry(const StatsEntry &other) = default;

        void compute() 
        {
            if (serie_.empty())
                return;

            sort(begin(serie_), end(serie_),
                [this](const TraceEntry &lhs, const TraceEntry &rhs) {
                    const auto duration_lhs = duration(lhs);
                    const auto duration_rhs = duration(rhs);

                    return duration_lhs < duration_rhs;
            });

            const auto size = serie_.size();
            const auto rank_99 = int { round(0.99 * (size - 1)) };
            const auto &entry_99 = serie_[rank_99];
            centile =  duration(entry_99);

            auto acc = [this](double current, const TraceEntry &entry)
                      {
                          return current + duration(entry);
                      };

            mean = accumulate(begin(serie_), end(serie_), 0.0, acc) / serie_.size();
            
            const auto medianIndex = size / 2;
            if (medianIndex % 2 == 0)
                median = duration(serie_[medianIndex]);
            else
                median = (duration(serie_[medianIndex]) + 
                          duration(serie_[medianIndex - 1])) / 2.0;

        }    

        std::string tag;
        double centile;
        double mean;
        double median;

        Json::Value toJson() const {
            Json::Value value;
            value["centile"] = centile;
            value["mean"] = mean;
            value["median"] = median;
            return value;
        }

        private:
            double duration(const TraceEntry &entry) {
                return std::chrono::duration_cast<std::chrono::milliseconds>(
                        entry.span.end - entry.span.start).count();
            }
            std::vector<TraceEntry> serie_;
    };

    struct ObjectStats {
        ObjectStats(const std::string &kind)
            : kind { kind }
        { }

        void addEntry(StatsEntry entry) {
            values.push_back(std::move(entry));
        }

        Json::Value toJson() const {
            Json::Value value;
            std::for_each(begin(values), end(values), [&](const StatsEntry &entry) {
                value[entry.tag] = entry.toJson();
            });

            return value;
        }


        std::string kind;
    private:
        std::vector<StatsEntry> values;
    };

    struct GlobalStats : public std::vector<ObjectStats> {
        Json::Value toJson() const {
            Json::Value root;
            std::for_each(begin(), end(), [&](const ObjectStats &stats) {
                root[stats.kind] = stats.toJson();
            });

            return root;
        }
    };

    void installRoutes() {
        auto &v0Router = restRouter.addSubRouter(
                "/v0", "Simple tracing REST interface");

        addRouteSyncReturn(
                v0Router, "/stats", { "GET" },
                "Returns raw statistics",
                "Accumulated statistics from collected traces",
                [](const GlobalStats &stats) { return stats.toJson(); },
                &TracingRestEndpoint::computeStats,
                this
                );
    }


    bool handleMessage(const std::string &message) {

        Json::Value root;
        Json::Reader reader;
        bool ok = reader.parse(message, root);
        if (!ok) {
            return false;
        }

        auto entry = TraceEntry::fromJson(root);
        entries[index] = std::move(entry);

        index = (index + 1) & (MaxEntries - 1);

        return true;
    }

    GlobalStats computeStats() const {
        typedef std::map<std::string, std::vector<TraceEntry>> TracingData;
        /* Maps object type (kind) to tracing data */
        std::map<std::string, TracingData> data;

        std::for_each(begin(entries), end(entries), [&](const TraceEntry &entry) {
            auto &tracingData = data[entry.context.kind];
            auto &vec = tracingData[entry.span.tag];
            vec.push_back(entry);
        });

        GlobalStats stats;

        for (const auto &kind: data) {
            auto tags = kind.second;

            ObjectStats objStats(kind.first);

            for (const auto &tag: tags) {
                objStats.addEntry(StatsEntry(tag.first, tag.second));
            }

            stats.push_back(objStats);

        }

        return stats;

    }

    void handleRequest(const ConnectionId &conn, const RestRequest &request) const
    {
        restRouter.handleRequest(conn, request);
    }
};

int main(int argc, const char *argv[]) {

    ServiceProxyArguments serviceArgs;

    std::string fifoPath;
    namespace po = boost::program_options;

    po::options_description options;
    options
        .add(serviceArgs.makeProgramOptions());

    options.add_options() 
        ("path,p", po::value<std::string>(&fifoPath),
              "path of the fifo where logs are stored");

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
    po::notify(vm);

    auto proxies = serviceArgs.makeServiceProxies();
    auto serviceName = serviceArgs.serviceName("tracing");
    TracingRestEndpoint endpoint(proxies, serviceName);
    endpoint.bindFixedHttpAddress("localhost", 3481);
    endpoint.start();
    endpoint.run(fifoPath);

}
