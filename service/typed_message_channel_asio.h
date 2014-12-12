/* typed_message_queue_asiof.h                                     -*- C++ -*-
   Wolfgang Sourdeau, 2 Dec 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

#pragma once

#include <mutex>
#include <queue>

#include <boost/asio/io_service.hpp>


namespace Datacratic {

/*****************************************************************************
 * TYPED MESSAGE QUEUE ASIO                                                  *
 *****************************************************************************/

/* Same as TypedMessageQueue, ported to boost::asio */
template<typename Message>
struct TypedMessageQueueAsio
{
    typedef std::function<void ()> OnNotify;

    TypedMessageQueueAsio(boost::asio::io_service & ioService,
                          size_t maxMessages = 0,
                          const OnNotify & onNotify = nullptr)
        : ioService_(ioService),
          maxMessages_(maxMessages), pending_(false),
          onNotify_(onNotify)
    {
    }

    void setOnNotify(const OnNotify & onNotify)
    {
        onNotify_ = onNotify;
    }

    virtual void onNotify()
    {
        if (onNotify_) {
            onNotify_();
        }
    }

    /* reset the maximum number of messages */
    void setMaxMessages(size_t count)
    {
        maxMessages_ = count;
    }

    /* push message into the queue */
    bool push_back(Message message)
    {
        Guard guard(queueLock_);

        if (maxMessages_ > 0 && queue_.size() >= maxMessages_) {
            return false;
        }

        queue_.emplace(std::move(message));
        if (!pending_) {
            pending_ = true;
            notifyReceiver();
        }

        return true;
    }

    /* returns up to "number" messages from the queue or all of them if 0 */
    std::vector<Message> pop_front(size_t number)
    {
        std::vector<Message> messages;
        Guard guard(queueLock_);

        size_t queueSize = queue_.size();
        if (number == 0 || number > queueSize) {
            number = queueSize;
        }
        messages.reserve(number);

        for (size_t i = 0; i < number; i++) {
            messages.emplace_back(std::move(queue_.front()));
            queue_.pop();
        }

        if (queue_.size() == 0) {
            pending_ = false;
        }

        return messages;
    }

    /* number of messages present in the queue */
    uint64_t size()
        const
    {
        return queue_.size();
    }

private:
    void notifyReceiver()
    {
        auto doNotify = [&] () {
            std::cerr << "doNotify\n";
            this->onNotify();
        };
        ioService_.post(doNotify);
    }

    boost::asio::io_service & ioService_;

    typedef std::mutex Mutex;
    typedef std::unique_lock<Mutex> Guard;
    Mutex queueLock_;
    std::queue<Message> queue_;
    size_t maxMessages_;

    /* notifications are pending */
    bool pending_;

    /* callback */
    OnNotify onNotify_;
};

} // namespace Datacratic
