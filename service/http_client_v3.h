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

    boost::asio::deadline_timer timeoutTimer_;
};


/* HTTPCLIENT */

struct HttpClientV3 : public HttpClientImpl {
    HttpClientV3(const std::string & baseUrl,
                 int numParallel, size_t queueSize);
    HttpClientV3(HttpClient && other) = delete;
    HttpClientV3(const HttpClient & other) = delete;

    ~HttpClientV3();

    /* HttpClientImpl */
    virtual void enableDebug(bool value);
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
        return (queue_ ? queue_->size() : 0);
    }

    HttpClient & operator = (HttpClient && other) = delete;
    HttpClient & operator = (const HttpClient & other) = delete;

private:
    typedef AsioTypedMessageQueue<HttpRequest> HttpRequestQueue;

    void handleQueueEvent();

    void handleHttpConnectionDone(HttpConnectionV3 * connection,
                                  const boost::system::error_code & result);

    HttpConnectionV3 * getConnection();
    void releaseConnection(HttpConnectionV3 * connection);

    std::string baseUrl_;
    boost::asio::ip::tcp::endpoint endpoint_;

    bool debug_;

    std::vector<std::shared_ptr<HttpConnectionV3>> allConnections_;
    std::vector<HttpConnectionV3 *> avlConnections_;
    size_t nextAvail_;

    std::unique_ptr<HttpRequestQueue> queue_;

    HttpConnectionV3::OnDone onHttpConnectionDone_;
};

} // namespace Datacratic
