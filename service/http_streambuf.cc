/** http_streambuf.cc
    Jeremy Barnes, 26 November 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include <atomic>
#include <boost/iostreams/stream_buffer.hpp>
#include "http_rest_proxy.h"
#include "jml/utils/ring_buffer.h"
#include <chrono>


using namespace std;


namespace Datacratic {

struct HttpStreamingDownloadSource {
    HttpStreamingDownloadSource(const std::string & urlStr)
    {
        impl.reset(new Impl(urlStr));
        impl->start();
    }

    typedef char char_type;
    struct category
        : //input_seekable,
        boost::iostreams::input,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag
    { };

    struct Impl {
        Impl(const std::string & urlStr)
            : proxy(urlStr), urlStr(urlStr), shutdown(false), dataQueue(100),
              eof(false), currentDone(0)
        {
            reset();
        }

        ~Impl()
        {
            stop();
        }

        HttpRestProxy proxy;
        std::string urlStr;

        atomic<bool> shutdown;
        exception_ptr lastExc;

        /* Data queue */
        ML::RingBufferSRMW<string> dataQueue;
        atomic<bool> eof;

        std::string current;
        size_t currentDone;

        vector<std::thread> threads; /* thread pool */

        /* cleanup all the variables that are used during reading, the
           "static" ones are left untouched */
        void reset()
        {
            shutdown = false;
            current = "";
            currentDone = 0;
            threads.clear();
        }

        void start()
        {
            threads.emplace_back(&Impl::runThread, this);
        }

        void stop()
        {
            shutdown = true;


            while (!dataQueue.tryPush("")) {
                string item;
                dataQueue.tryPop(item, 0.001);
            }

            for (thread & th: threads) {
                th.join();
            }

            threads.clear();
        }

        /* reader thread */
        std::streamsize read(char_type* s, std::streamsize n)
        {
            if (lastExc) {
                rethrow_exception(lastExc);
            }

            if (eof)
                return -1;

            if (currentDone == current.size()) {
                // Get some more data
                current = dataQueue.pop();
                currentDone = 0;

                if (current.empty()) {
                    if (lastExc) rethrow_exception(lastExc);
                    eof = true;
                    return -1;  // shutdown or empty
                }
            }
            
            if (lastExc) {
                rethrow_exception(lastExc);
            }
            
            size_t toDo = min<size_t>(current.size() - currentDone, n);
            const char_type * start = current.c_str() + currentDone;
            std::copy(start, start + toDo, s);
            
            currentDone += toDo;

            return toDo;
        }

        void runThread()
        {
            try {
                int errorCode =-1;
                std::string errorBody;
                bool error = false;
                auto onData = [&] (const std::string & data)
                    {
                        if (error) {
                            errorBody = data;
                            return false;
                        }
                        if (shutdown)
                            return false;
                        while (!shutdown && !dataQueue.tryPush(data)) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        }
                        return !shutdown;
                    };

                auto onHeader = [&] (const HttpHeader & header)
                    {
                        if (shutdown)
                            return false;

                        //cerr << "got header " << header << endl;
                        errorCode = header.responseCode();

                        if (header.responseCode() != 200)
                            error = true;

                        return !shutdown;
                    };

                auto resp = proxy.get("", {}, {}, -1 /* timeout */,
                                      false /* exceptions */,
                                      onData, onHeader);
                
                if (shutdown)
                    return;

                if (resp.code() != 200) {
                    throw ML::Exception("HTTP code %d reading %s\n\n%s",
                                        errorCode, urlStr.c_str(),
                                        string(errorBody, 0, 1024).c_str());
                }
                
                dataQueue.tryPush("");
                
            } catch (const std::exception & exc) {
                lastExc = std::current_exception();
                dataQueue.tryPush("");
            }
        }

    };

    std::shared_ptr<Impl> impl;

    std::streamsize read(char_type* s, std::streamsize n)
    {
        return impl->read(s, n);
    }

    bool is_open() const
    {
        return !!impl;
    }

    void close()
    {
        impl.reset();
    }
};

std::unique_ptr<std::streambuf>
makeHttpStreamingDownload(const std::string & uri)
{
    std::unique_ptr<std::streambuf> result;
    result.reset(new boost::iostreams::stream_buffer<HttpStreamingDownloadSource>
                 (HttpStreamingDownloadSource(uri),
                  131072));
    return result;
}

/** Register Http with the filter streams API so that a filter_stream can be
    used to treat an Http object as a simple stream.
*/
struct RegisterHttpHandler {
    static std::pair<std::streambuf *, bool>
    getHttpHandler(const std::string & scheme,
                 const std::string & resource,
                 std::ios_base::open_mode mode,
                 const std::map<std::string, std::string> & options,
                 const ML::OnUriHandlerException & onException)
    {
        string::size_type pos = resource.find('/');
        if (pos == string::npos)
            throw ML::Exception("unable to find http bucket name in resource "
                                + resource);
        string bucket(resource, 0, pos);

        if (mode == ios::in) {
            return make_pair(makeHttpStreamingDownload(scheme+"://"+resource).release(),
                             true);
        }
        else if (mode == ios::out) {
            throw ML::Exception("Can't currently upload files via HTTP/HTTPs");
        }
        else throw ML::Exception("no way to create http handler for non in/out");
    }
    
    RegisterHttpHandler()
    {
        ML::registerUriHandler("http", getHttpHandler);
        ML::registerUriHandler("https", getHttpHandler);
    }

} registerHttpHandler;

} // namespace Datacratic
