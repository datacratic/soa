/* http_client.h                                                   -*- C++ -*-
   Wolfgang Sourdeau, January 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   An asynchronous HTTP client.

   HttpClient is meant to provide a featureful, generic and asynchronous HTTP
   client class. It supports strictly asynchronous (non-blocking) operations,
   HTTP pipelining and concurrent requests while enabling streaming responses
   via a callback mechanism. It is meant to be subclassed whenever a
   synchronous interface or a one-shot response mechanism is required. In
   general, the code should be complete enough that existing and similar
   classes could be subclassed gradually (HttpRestProxy, s3 internals). As a
   generic class, it does not make assumptions on the transferred contents.
   Finally, it is based on the interface of HttpRestProxy.

   Caveat:
   - cannot be used with a multi-threaded loop yet
   - has not been tweaked for performance yet
   - since those require header interpretation, there is not support for
     cookies per se
*/

#pragma once

/* TODO:
   blockers:
   - connection timeout (Curl style)
   - chunked encoding

   nice to have:
   - connect timeout
   - activity timeout
   - parser:
     - needs better validation (header key size, ...)
   - compression
   - auto disconnect (keep-alive)
   - SSL support
   - pipelining
 */

#include <string>
#include <vector>

#include "soa/jsoncpp/value.h"
#include "soa/service/http_client.h"
#include "soa/service/http_header.h"
#include "soa/service/http_parsers.h"
#include "soa/service/message_loop.h"
#include "soa/service/typed_message_channel.h"
#include "soa/service/tcp_client.h"


namespace Datacratic {

/* HTTP CONNECTION */

struct HttpConnection : TcpClient {
    typedef std::function<void (TcpConnectionCode)> OnDone;

    enum HttpState {
        IDLE,
        PENDING
    };

    HttpConnection();

    HttpConnection(const HttpConnection & other) = delete;

    ~HttpConnection();

    void clear();
    void perform(HttpRequest && request);

    const HttpRequest & request() const
    {
        return request_;
    }

    OnDone onDone;

private:
    /* tcp_socket overrides */
    virtual void onClosed(bool fromPeer,
                          const std::vector<std::string> & msgs);
    virtual void onReceivedData(const char * data, size_t size);
    virtual void onException(const std::exception_ptr & excPtr);

    void onParserResponseStart(const std::string & httpVersion, int code);
    void onParserHeader(const char * data, size_t size);
    void onParserData(const char * data, size_t size);
    void onParserDone(bool onClose);

    void startSendingRequest();

    void handleEndOfRq(TcpConnectionCode code, bool requireClose);
    void finalizeEndOfRq(TcpConnectionCode code);

    HttpResponseParser parser_;

    HttpState responseState_;
    HttpRequest request_;
    bool requestEnded_;

    /* Connection: close */
    TcpConnectionCode lastCode_;

    /* request timeouts */
    void armRequestTimer();
    void cancelRequestTimer();
    void handleTimeoutEvent(const ::epoll_event & event);

    int timeoutFd_;
};


/* HTTPCLIENT */

struct HttpClientV2 : public HttpClientImpl {
    HttpClientV2(const std::string & baseUrl,
                 int numParallel, size_t queueSize);
    HttpClientV2(HttpClient && other) = delete;
    HttpClientV2(const HttpClient & other) = delete;

    ~HttpClientV2();

    /* AsyncEventSource */
    virtual int selectFd() const;
    virtual bool processOne();

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

    void handleHttpConnectionDone(HttpConnection * connection,
                                  TcpConnectionCode result);

    HttpConnection * getConnection();
    void releaseConnection(HttpConnection * connection);

    MessageLoop loop_;

    std::string baseUrl_;

    bool debug_;

    std::vector<HttpConnection *> avlConnections_;
    size_t nextAvail_;

    TypedMessageQueue<HttpRequest> queue_; /* queued requests */

    HttpConnection::OnDone onHttpConnectionDone_;
};

} // namespace Datacratic
