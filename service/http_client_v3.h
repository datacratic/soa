/* http_client.h                                                   -*- C++ -*-
   Wolfgang Sourdeau, November 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   
*/

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

#include "soa/jsoncpp/value.h"
#include "soa/service/http_client.h"
#include "soa/service/http_header.h"
#include "soa/service/http_parsers.h"
#include "soa/service/typed_message_channel_asio.h"


namespace Datacratic {

/* HTTP CONNECTION */

struct HttpConnectionV3 {
    typedef std::function<void (boost::system::error_code)> OnDone;

    enum HttpState {
        IDLE,
        PENDING
    };

    HttpConnectionV3(boost::asio::io_service & ioService,
                     const boost::asio::ip::tcp::endpoint & endpoint);

    HttpConnectionV3(const HttpConnectionV3 & other) = delete;

    ~HttpConnectionV3();

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

    bool queueEnabled()
        const
    {
        return connected_;
    }

    typedef std::function<void(const boost::system::error_code &,
                               std::size_t)> OnWritten;
    void write(const char * data, size_t size, const OnWritten & onWritten);

    /* AsyncWriterSource methods */


    /* tcp_socket overrides */
    void onClosed(bool fromPeer,
                  const std::vector<std::string> & msgs);
    void onReceivedData(const char * data, size_t size);
    typedef std::function<void(const boost::system::error_code & ec,
                               size_t bufferSize)> OnReceivedDataFn;
    OnReceivedDataFn onReceivedDataFn_;

    void onException(const std::exception_ptr & excPtr);

    void onParserResponseStart(const std::string & httpVersion, int code);
    void onParserHeader(const char * data, size_t size);
    void onParserData(const char * data, size_t size);
    void onParserDone(bool onClose);

    void startSendingRequest();

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
    void handleTimeoutEvent(const ::epoll_event & event);

    int timeoutFd_;
};


/* HTTPCLIENT */

struct HttpClientV3
/* : public HttpClientImpl*/ {
    HttpClientV3(boost::asio::io_service & ioService,
                 const std::string & baseUrl,
                 int numParallel = 1024, size_t queueSize = 0);
    HttpClientV3(HttpClient && other) = delete;
    HttpClientV3(const HttpClient & other) = delete;

    ~HttpClientV3();

    // /* AsyncEventSource */
    // virtual int selectFd() const;
    // virtual bool processOne();

    /* HttpClient */
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


    /* HttpClientImpl */
    void enableSSLChecks(bool value);
    void sendExpect100Continue(bool value);
    void enableTcpNoDelay(bool value);
    void enablePipelining(bool value);

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
        return queue_.size();
    }

    HttpClient & operator = (HttpClient && other) = delete;
    HttpClient & operator = (const HttpClient & other) = delete;

private:
    void handleQueueEvent();

    void handleHttpConnectionDone(HttpConnectionV3 * connection,
                                  const boost::system::error_code & result);

    HttpConnectionV3 * getConnection();
    void releaseConnection(HttpConnectionV3 * connection);

    // MessageLoop loop_;
    boost::asio::io_service & ioService_;

    std::string baseUrl_;
    boost::asio::ip::tcp::endpoint endpoint_;

    bool debug_;

    std::vector<std::shared_ptr<HttpConnectionV3>> allConnections_;
    std::vector<HttpConnectionV3 *> avlConnections_;
    size_t nextAvail_;

    TypedMessageQueueAsio<HttpRequest> queue_; /* queued requests */

    HttpConnectionV3::OnDone onHttpConnectionDone_;
};

} // namespace Datacratic
