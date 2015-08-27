#include <iostream>
#include "googleurl/src/gurl.h"
#include "jml/utils/exc_assert.h"
#include "soa/types/url.h"

#include "boost/asio/write.hpp"

#include "asio_tcp_service.h"

using namespace std;
using namespace boost;
using namespace Datacratic;


AsioTcpLoop::
AsioTcpLoop()
    : shutdown_(false)
{
    auto runLoopFn = [&] {
        asio::io_service::work work(ioService_);
        cerr << "worker running\n";
        ioService_.run();
        cerr << "worker existing\n";
    };
    loopThread_.reset(new thread(runLoopFn));
}

AsioTcpLoop::
~AsioTcpLoop()
{
    shutdown();
    loopThread_->join();
}

void
AsioTcpLoop::
associate(std::shared_ptr<AsioTcpHandler> handler)
{
    auto bootstrapHandler = [=] () {
        associatedHandlers_.insert(handler);
        cerr << "bootstrapping handler\n";
        handler->setLoop(this);
        handler->bootstrap();
    };
    ioService_.post(bootstrapHandler);
}

/* FIXME: inefficient implementation */
void
AsioTcpLoop::
dissociate(AsioTcpHandler * handler)
{
    for (auto & handlerPtr: associatedHandlers_) {
        if (handlerPtr.get() == handler) {
            associatedHandlers_.erase(handlerPtr);
            cerr << "dissociated handler\n";
            break;
        }
    }
}

void
AsioTcpLoop::
shutdown()
{
    if (shutdown_) return;
    shutdown_ = true;
    auto noopFn = [&] {
        cerr << "noopFn\n";
    };
    ioService_.post(noopFn);
    ioService_.stop();
}


/****************************************************************************/
/* ASIO TCP ACCEPTOR                                                        */
/****************************************************************************/

AsioTcpAcceptor::
AsioTcpAcceptor(asio::io_service & ioService,
                const std::string & hostname, int port,
                const OnNewConnection & onNewConnectionFn,
                int numWorkers)
    : acceptor_(ioService),
      effectivePort_(-1),
      acceptCnt_(0),
      nextSocket_(ioService)
{
    ExcAssert(numWorkers > 0);
    for (int i = 0; i < numWorkers; i++) {
        loops_.emplace_back(new AsioTcpLoop());
    }

    onNewConnection_ = onNewConnectionFn;

    asio::ip::tcp::endpoint endpoint(asio::ip::tcp::v4(), port);
    if (!hostname.empty()) {
        endpoint.address(asio::ip::address_v4::from_string(hostname));
    }
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(asio::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(100);
}

AsioTcpAcceptor::
~AsioTcpAcceptor()
{
}

void
AsioTcpAcceptor::
bootstrap()
{
    accept();
}

std::shared_ptr<AsioTcpHandler>
AsioTcpAcceptor::
onNewConnection(boost::asio::ip::tcp::socket && socket)
{
    return onNewConnection_(std::move(socket));
}

void
AsioTcpAcceptor::
accept()
{
    AsioTcpLoop & loop = *loops_[acceptCnt_];
    acceptCnt_ = (acceptCnt_ + 1) % loops_.size();
    nextSocket_ = asio::ip::tcp::socket(loop.ioService());
    auto onAcceptFn = [&] (const boost::system::error_code & ec) {
        if (ec) {
            cerr << "exception in accept: " + ec.message() + "\n";
        }
        else {
            auto newConn = onNewConnection(move(nextSocket_));
            loop.associate(newConn);
            accept();
        }
    };
    acceptor_.async_accept(nextSocket_, onAcceptFn);
}


/****************************************************************************/
/* ASIO TCP HANDLER                                                         */
/****************************************************************************/

AsioTcpHandler::
AsioTcpHandler(boost::asio::ip::tcp::socket && socket)
    : loop_(nullptr), socket_(move(socket)), recvBufferSize_(262144)
{
    recvBuffer_ = new char[recvBufferSize_];
    onReadSome_ = [&] (const system::error_code & ec, size_t bufferSize) {
        if (ec) {
            this->onReceiveError(ec, bufferSize);
        }
        else {
            this->onReceivedData(recvBuffer_, bufferSize);
        }
    };
}

AsioTcpHandler::
~AsioTcpHandler()
{
    delete recvBuffer_;
}

void
AsioTcpHandler::
bootstrap()
{
}

void
AsioTcpHandler::
close()
{
}

void
AsioTcpHandler::
requestClose(OnClose onClose)
{
    auto doCloseFn = [=] () {
        close();
        if (onClose) {
            onClose();
        }
        detach();
    };
    socket_.get_io_service().post(doCloseFn);
}

void
AsioTcpHandler::
detach()
{
    if (loop_) {
        AsioTcpLoop * oldLoop = loop_;
        loop_ = nullptr;
        oldLoop->dissociate(this);
    }
}

void
AsioTcpHandler::
requestReceive()
{
    socket_.async_read_some(asio::buffer(recvBuffer_, recvBufferSize_),
                            onReadSome_);
}

void
AsioTcpHandler::
requestWrite(string data, OnWritten onWritten)
{
    writeData_ = move(data);
    auto writeCompleteCond = [&] (const system::error_code & ec,
                                  std::size_t written) {
        // ::fprintf(stderr, "written: %d, total: %lu\n"
        //           written, totalSize);
        return written == writeData_.size();
    };
    asio::const_buffers_1 writeBuffer(writeData_.c_str(),
                                      writeData_.size());
    async_write(socket_, writeBuffer, writeCompleteCond, onWritten);
}
