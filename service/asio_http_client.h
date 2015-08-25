/* http_client.h                                                   -*- C++ -*-
   Wolfgang Sourdeau, November 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   
*/

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/system/error_code.hpp>

#include "soa/jsoncpp/value.h"
#include "soa/service/http_client.h"
#include "soa/service/http_header.h"
#include "soa/service/http_parsers.h"
#include "soa/service/asio_typed_message_queue.h"


namespace Datacratic {

/* HTTP CONNECTION */

struct AsioHttpConnection {
    typedef std::function<void (boost::system::error_code)> OnDone;

    enum HttpState {
        IDLE,
        PENDING
    };

    AsioHttpConnection(boost::asio::io_service & ioService);
    AsioHttpConnection(const AsioHttpConnection & other) = delete;

    ~AsioHttpConnection();

    void clear();
    void perform(HttpRequest && request);

    const HttpRequest & request() const
    {
        return request_;
    }

    OnDone onDone;

    // boost::asio::io_service & ioService_;
    boost::asio::ip::tcp::socket socket_;
    bool connected_;
    boost::asio::ip::tcp::endpoint endpoint_;

    /* AsyncWriterSource methods */
    void requestClose();
    void doClose();

    typedef std::function<void(const boost::system::error_code &,
                               std::size_t)> OnWritten;
    void write(const char * data, size_t size, const OnWritten & onWritten);

    /* AsyncWriterSource methods */


    /* tcp_socket overrides */
    void onClosed(bool fromPeer,
                  const std::vector<std::string> & msgs);
    void onWrittenData(size_t size);
    void onReceivedData(size_t size);
    void onWriteError(const boost::system::error_code & ec,
                      size_t bufferSize);
    void onReceiveError(const boost::system::error_code & ec,
                        size_t bufferSize);

    typedef std::function<void(const boost::system::error_code & ec,
                               size_t bufferSize)> OnReadSome;
    OnReadSome onReadSome_;

    void onException(const std::exception_ptr & excPtr);

    void onParserResponseStart(const std::string & httpVersion, int code);
    void onParserHeader(const char * data, size_t size);
    void onParserData(const char * data, size_t size);
    void onParserDone(bool onClose);

    void startSendingRequest();
    void resolveAndConnect();
    void connect();

    void handleEndOfRq(const boost::system::error_code & code,
                       bool requireClose);
    void finalizeEndOfRq(const boost::system::error_code & code);

    HttpResponseParser parser_;
    std::string rqData_;

    HttpState responseState_;
    HttpRequest request_;
    bool requestEnded_;
    bool parsingEnded_;

    char * recvBuffer_;
    size_t recvBufferSize_;

    /* Connection: close */
    boost::system::error_code lastCode_;

    /* request timeouts */
    void armRequestTimer();
    void cancelRequestTimer();
    void handleTimeoutEvent(const boost::system::error_code & ec);

    boost::asio::ip::tcp::resolver resolver_;
    std::vector<boost::asio::ip::tcp::endpoint> endpoints_;
    int currentEndpoint_;
    boost::asio::deadline_timer timeoutTimer_;
};


/* HTTPCLIENT */

struct AsioHttpClient {
    AsioHttpClient(boost::asio::io_service & service,
                   const std::string & baseUrl,
                   int numParallel = 1, size_t queueSize = 0);
    AsioHttpClient(HttpClient && other) = delete;
    AsioHttpClient(const HttpClient & other) = delete;

    ~AsioHttpClient();

    /* HttpClientImpl */
    virtual void enableDebug(bool value);
    void enableSSLChecks(bool value);
    void enableTcpNoDelay(bool value);
    void enablePipelining(bool value);

    /** Performs a GET request, with "resource" as the location of the
     *  resource on the server indicated in "baseUrl". Query parameters
     *  should preferably be passed via "queryParams".
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool get(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("GET", resource, callbacks,
                              HttpRequest::Content(),
                              queryParams, headers, timeout);
    }

    /** Performs a POST request, using similar parameters as get with the
     * addition of "content" which defines the contents body and type.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool post(const std::string & resource,
              const std::shared_ptr<HttpClientCallbacks> & callbacks,
              const HttpRequest::Content & content = HttpRequest::Content(),
              const RestParams & queryParams = RestParams(),
              const RestParams & headers = RestParams(),
              int timeout = -1)
    {
        return enqueueRequest("POST", resource, callbacks, content,
                              queryParams, headers, timeout);
    }

    /** Performs a PUT request in a similar fashion to "post" above.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool put(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const HttpRequest::Content & content = HttpRequest::Content(),
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("PUT", resource, callbacks, content,
                              queryParams, headers, timeout);
    }

    /** Performs a DELETE request. Note that this method cannot be named
     * "delete", which is a reserved keyword in C++.
     *
     *  Returns "true" when the request could successfully be enqueued.
     */
    bool del(const std::string & resource,
             const std::shared_ptr<HttpClientCallbacks> & callbacks,
             const RestParams & queryParams = RestParams(),
             const RestParams & headers = RestParams(),
             int timeout = -1)
    {
        return enqueueRequest("DELETE", resource, callbacks,
                              HttpRequest::Content(),
                              queryParams, headers, timeout);
    }

    bool enqueueRequest(const std::string & verb,
                        const std::string & resource,
                        const std::shared_ptr<HttpClientCallbacks> & callbacks,
                        const HttpRequest::Content & content,
                        const RestParams & queryParams,
                        const RestParams & headers,
                        int timeout = -1);

    size_t queuedRequests()
        const
    {
        return (queue_ ? queue_->size() : 0);
    }

    HttpClient & operator = (HttpClient && other) = delete;
    HttpClient & operator = (const HttpClient & other) = delete;

private:
    typedef AsioTypedMessageQueue<HttpRequest> HttpRequestQueue;

    void handleQueueEvent();

    void handleHttpConnectionDone(AsioHttpConnection * connection,
                                  const boost::system::error_code & result);

    AsioHttpConnection * getConnection();
    void releaseConnection(AsioHttpConnection * connection);

    std::string baseUrl_;
    boost::asio::ip::tcp::endpoint endpoint_;

    bool debug_;

    std::vector<std::shared_ptr<AsioHttpConnection>> allConnections_;
    std::vector<AsioHttpConnection *> avlConnections_;
    size_t nextAvail_;

    std::unique_ptr<HttpRequestQueue> queue_;

    AsioHttpConnection::OnDone onHttpConnectionDone_;
};

} // namespace Datacratic
