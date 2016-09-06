/* named_endpoint_test.cc                                          -*- C++ -*-
   Jeremy Barnes, 24 September 2012
   Copyright (c) 2012 Datacratic Inc.  All rights reserved.

   Test for named endpoint.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <boost/make_shared.hpp>
#include <sys/socket.h>
#include "jml/utils/guard.h"
#include "jml/arch/exception_handler.h"
#include "jml/utils/vector_utils.h"
#include "jml/arch/timers.h"
#include <thread>
#include "soa/service/zmq_utils.h"
#include "soa/service/zmq_named_pub_sub.h"

using namespace std;
using namespace ML;
using namespace Datacratic;

struct Publisher : public ServiceBase, public ZmqNamedPublisher {

    Publisher(const std::string & name,
              std::shared_ptr<ServiceProxies> proxies)
        : ServiceBase(name, proxies),
          ZmqNamedPublisher(proxies->zmqContext)
    {
    }

    ~Publisher()
    {
        unregisterServiceProvider(serviceName(), { "publisher" });
        shutdown();
    }

    void init()
    {
        ZmqNamedPublisher::init(getServices()->config, serviceName() + "/publish");
        registerServiceProvider(serviceName(), { "publisher" });
    }
};

BOOST_AUTO_TEST_CASE( test_named_publisher )
{
    auto proxies = std::make_shared<ServiceProxies>();
    // proxies->useZookeeper(ML::format("localhost:%d", zookeeper.getPort()));

    ZmqNamedSubscriber subscriber(*proxies->zmqContext);
    subscriber.init(proxies->config);

    int numMessages = 0;
    vector<vector<string> > subscriberMessages;

    subscriber.messageHandler = [&] (const std::vector<zmq::message_t> & message)
        {
            vector<string> msg2;
            for (unsigned i = 0;  i < message.size();  ++i) {
                msg2.push_back(message[i].toString());
            }
            cerr << "got subscriber message " << msg2 << endl;
            
            subscriberMessages.push_back(msg2);
            ++numMessages;
            futex_wake(numMessages);
        };
    
    subscriber.connectToEndpoint("pub/publish");
    subscriber.start();
    subscriber.subscribe("hello");

    int numIter = 1;
    //numIter = 10;  // TODO: test fails here

    for (unsigned i = 0;  i < numIter;  ++i) {
        ML::sleep(0.1);

        cerr << endl << endl << endl << endl;

        BOOST_CHECK_NE(subscriber.getConnectionState(),
                       ZmqNamedSubscriber::CONNECTED);
        
        Publisher pub("pub", proxies);

        pub.init();
        pub.bindTcp();
        pub.start();

        proxies->config->dump(cerr);

        ML::sleep(0.1);

        BOOST_CHECK_EQUAL(subscriber.getConnectionState(),
                          ZmqNamedSubscriber::CONNECTED);

#if 1
        {
            //auto subp = new ZmqNamedSubscriber(*proxies->zmqContext);
            //auto & sub = *subp;

            ZmqNamedSubscriber sub(*proxies->zmqContext);
            sub.init(proxies->config);
            sub.start();

            vector<vector<string> > subscriberMessages;
            volatile int numMessages = 0;

            auto onSubscriptionMessage = [&] (const std::vector<zmq::message_t> & message)
                {
                    vector<string> msg2;
                    for (unsigned i = 0;  i < message.size();  ++i) {
                        msg2.push_back(message[i].toString());
                    }
                    cerr << "got message " << msg2 << endl;

                    subscriberMessages.push_back(msg2);
                    ++numMessages;
                    futex_wake(numMessages);
                };
            

            sub.messageHandler = onSubscriptionMessage;

            sub.connectToEndpoint("pub/publish");

            // Busy wait (for now)
            for (unsigned i = 0;  subscriber.getConnectionState() != ZmqNamedSubscriber::CONNECTED;  ++i) {
                ML::sleep(0.01);
                if (i && i % 10 == 0)
                    cerr << "warning: waited " << i / 10 << "ds for subscriber to connect" << endl;
                //if (i == 200)
                //    throw ML::Exception("no connection in 2 seconds");
            }

            sub.subscribe("hello");

            // Give the subscription message time to percolate through
            ML::sleep(0.5);

            // Publish some messages
            pub.publish("hello", "world");
            pub.publish("dog", "eats", "dog");
            pub.publish("hello", "stranger");

            cerr << "published" << endl;

            // Wait until they are received
            for (;;) {
                int nm = numMessages;
                if (nm == 2) break;
                ML::futex_wait(numMessages, nm);
            }

            BOOST_CHECK_EQUAL(subscriberMessages.size(), 2);
            BOOST_CHECK_EQUAL(subscriberMessages.at(0), vector<string>({ "hello", "world"}) );
            BOOST_CHECK_EQUAL(subscriberMessages.at(1), vector<string>({ "hello", "stranger"}) );

            sub.shutdown();
        }
#else
        // Publish some messages
        pub.publish("hello", "world");
        pub.publish("dog", "eats", "dog");
        pub.publish("hello", "stranger");
#endif

        ML::sleep(0.1);

        cerr << "unregistered publisher" << endl;

        pub.shutdown();
    }

    ML::sleep(0.1);

    cerr << "got a total of " << numMessages << " subscriber messages" << endl;

    // Check that it got all of the messages
    BOOST_CHECK_EQUAL(numMessages, numIter * 2);
}
