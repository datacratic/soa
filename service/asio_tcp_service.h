/* tcp_service.h                                                   -*- C++ -*-
   Wolfgang Sourdeau, November 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.

   An asynchronous HTTP service.
*/

#pragma once

#include <memory>
#include <functional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>


namespace Datacratic {

struct AsioTcpHandler;


/****************************************************************************/
/* ASIO TCP LOOP                                                            */
/****************************************************************************/

struct AsioTcpLoop {
    AsioTcpLoop();
    ~AsioTcpLoop();

    boost::asio::io_service & ioService()
    {
        return ioService_;
    }

    void associate(std::shared_ptr<AsioTcpHandler> handler);
    void dissociate(AsioTcpHandler * handler);

private:
    void shutdown();

    boost::asio::io_service ioService_;
    bool shutdown_;
    std::unique_ptr<std::thread> loopThread_;
    std::set<std::shared_ptr<AsioTcpHandler> > associatedHandlers_;
};


/****************************************************************************/
/* ASIO TCP ACCEPTOR                                                        */
/****************************************************************************/

struct AsioTcpAcceptor {
    typedef std::function<std::shared_ptr<AsioTcpHandler>
                          (boost::asio::ip::tcp::socket && socket)>
        OnNewConnection;

    AsioTcpAcceptor(boost::asio::io_service & ioService,
                    const std::string & hostname, int port,
                    const OnNewConnection & onNewHandler, int nbrWorkers);
    virtual ~AsioTcpAcceptor();

    void bootstrap();

    int effectivePort()
        const
    {
        return effectivePort_;
    }

    virtual std::shared_ptr<AsioTcpHandler>
        onNewConnection(boost::asio::ip::tcp::socket && socket);

private:
    void accept();

    boost::asio::ip::tcp::acceptor acceptor_;

    int effectivePort_;
    int acceptCnt_;

    std::vector<std::unique_ptr<AsioTcpLoop> > loops_;

    boost::asio::ip::tcp::socket nextSocket_;

    OnNewConnection onNewConnection_;
};


/****************************************************************************/
/* ASIO TCP HANDLER                                                         */
/****************************************************************************/

struct AsioTcpHandler {
    typedef std::function<void ()> OnClose;
    typedef std::function<void (const boost::system::error_code & ec,
                                size_t)> OnWritten;

    AsioTcpHandler(boost::asio::ip::tcp::socket && socket);
    virtual ~AsioTcpHandler();

    virtual void bootstrap();

    void disableNagle();

    void close();
    void requestClose(OnClose onClose = nullptr);

    void requestWrite(std::string data, OnWritten onWritten = nullptr);

    void requestReceive();
    virtual void onReceivedData(const char * buffer, size_t bufferSize) = 0;
    virtual void onReceiveError(const boost::system::error_code & ec,
                                size_t bufferSize) = 0;

    AsioTcpHandler(const AsioTcpHandler & other) = delete;
    AsioTcpHandler & operator = (const AsioTcpHandler & other) = delete;

    void setLoop(AsioTcpLoop * loop)
    {
        loop_ = loop;
    }
    void detach();

private:
    AsioTcpLoop * loop_;

    boost::asio::ip::tcp::socket socket_;
    char * recvBuffer_;
    size_t recvBufferSize_;

    std::string writeData_;

    typedef std::function<void(const boost::system::error_code & ec,
                               size_t bufferSize)> OnReadSome;
    OnReadSome onReadSome_;
};

} // namespace Datacratic
