/* hdfs.cc
   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

#include <memory>
#include <string>
#include <boost/iostreams/filtering_stream.hpp>

#include "hdfs/hdfs.h"

#include "googleurl/src/url_util.h"
#include "jml/arch/exception.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/filter_streams.h"
#include "jml/utils/guard.h"
#include "soa/types/url.h"

using namespace std;
using namespace Datacratic;

constexpr int DEFAULT_PORT(50070);

namespace {

struct HDFSStreamingDownloadSource {
    HDFSStreamingDownloadSource(const string & urlStr)
    {
        impl.reset(new Impl(urlStr));
    }

    typedef char char_type;
    struct category
        : //input_seekable,
        boost::iostreams::input,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag
    { };

    struct Impl {
        Impl(const string & urlStr)
        {
            Url url(urlStr);

            string hostname = url.host();
            int port = url.port();
            if (port == -1) {
                port = DEFAULT_PORT;
            }
            string username = url.username();
            string filename = url.path();

            ExcAssertEqual(url.scheme(), "hdfs");
            ExcAssert(!filename.empty());

            hdfsBuilder * builder = hdfsNewBuilder();
            string baseUrl = "hdfs://" + hostname + ":" + to_string(port);
            hdfsBuilderSetNameNode(builder, baseUrl.c_str());

            if (!username.empty()) {
                hdfsBuilderSetUserName(builder, username.c_str());
            }

            bool ok(false);
            ML::Call_Guard guard([&] { if (!ok) cleanup(); });

            fsHandle_ = hdfsBuilderConnect(builder);
            ExcAssert(fsHandle_ != nullptr);

            if (hdfsExists(fsHandle_, filename.c_str())) {
                throw ML::Exception("file does not exist");
            }

            fileHandle_ = hdfsOpenFile(fsHandle_, filename.c_str(), O_RDONLY, 0,
                                       0, 0);
            ExcAssert(fileHandle_ != nullptr);

            ok = true;
        }

        ~Impl()
        {
            cleanup();
        }

        void cleanup()
            noexcept
        {
            if (fileHandle_) {
                hdfsCloseFile(fsHandle_, fileHandle_);
                fileHandle_ = nullptr;
            }
            if (fsHandle_) {
                hdfsDisconnect(fsHandle_);
                fsHandle_ = nullptr;
            }
        }

        streamsize read(char_type * s, streamsize n)
        {
            tSize readRes;

            {
                JML_TRACE_EXCEPTIONS(false);
                readRes = hdfsRead(fsHandle_, fileHandle_, s, n);
            }
            if (readRes == -1) {
                throw ML::Exception(readRes, "hdfsRead");
            }

            return readRes > 0 ? readRes : -1;
        }

    private:
        hdfsFS fsHandle_;
        hdfsFile fileHandle_;
        // tOffset fileSize_;
        // tOffset offset_;
    };

    shared_ptr<Impl> impl;

    streamsize read(char_type* s, streamsize n)
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


/** Register HDFS with the filter streams API.
*/
struct RegisterHDFSHandler {
    static pair<streambuf *, bool>
    getHDFSHandler(const string & scheme,
                   const string & resource,
                   ios_base::open_mode mode,
                   const map<string, string> & options,
                   const ML::OnUriHandlerException & onException)
    {
        string::size_type pos = scheme.find("hdfs://");
        if (pos != string::npos)
            throw ML::Exception("malformed hdfs url");
        string url = "hdfs://" + resource;

        if (mode == ios::in) {
            streambuf * result
                = new boost::iostreams::stream_buffer<HDFSStreamingDownloadSource>(HDFSStreamingDownloadSource(url), 131072);
            return make_pair(result, true);
        }
        else if (mode == ios::out) {
            throw ML::Exception("stream writing not supported yet");
        }
        else {
            throw ML::Exception("no way to create HDFS handler for non in/out");
        }
    }

    RegisterHDFSHandler()
    {
        /* this enables googleuri to parse our urls properly */
        url_util::AddStandardScheme("hdfs");
        ML::registerUriHandler("hdfs", getHDFSHandler);
    }

} registerHDFSHandler;

}
