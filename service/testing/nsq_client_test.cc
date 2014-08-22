#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <thread>
#include <boost/test/unit_test.hpp>

#include "jml/arch/timers.h"
// #include "jml/utils/string_functions.h"

#include "soa/service/message_loop.h"
#include "soa/service/nsq_client.h"

using namespace std;
using namespace Datacratic;


const int numMessages(50000);

void
doPublisherThread()
{
    MessageLoop loop;
    loop.start();

    int closed(true);
    auto onClosed = [&] (bool fromPeer,
                         const std::vector<std::string> & msgs) {
        cerr << "publisher: disconnected\n";
        closed = true;
        ML::futex_wake(closed);
    };

    auto client = make_shared<NsqClient>(onClosed);
    loop.addSource("client", client);
    client->init("http://127.0.0.1:4150");
    cerr << "publisher: connect...\n";
    auto result = client->connectSync();
    if (result != ConnectionResult::Success) {
        throw ML::Exception("connection error");
    }
    closed = false;

    int numDone(0);
    auto onPub = [&] (const NsqFrame & response) {
        numDone++;
        if (numDone == numMessages) {
            cerr << "publisher: received response for all published messages\n";
            ML::futex_wake(numDone);
        }
    };

    auto onIdentify = [&] (const NsqFrame & response) {
        cerr << "publisher: onIdentify\n";
        for (int i = 0; i < numMessages; i++) {
            client->pub("a-topic",
                        "this is some interesting message nr " + to_string(i),
                        onPub);
        }
        cerr << "publisher: published everything\n";
    };
    cerr << "publisher: identify...\n";
    client->identify(onIdentify);
    
    cerr << "publisher: waiting for all messages to be sent...\n";
    while (numDone < numMessages) {
        cerr << "publisher: numDone = " + to_string(numDone) + "\n";
        ML::sleep(2.0);
    }

    client->requestClose();

    cerr << "publisher: waiting for close state...\n";
    while (!closed) {
        int old = closed;
        ML::futex_wait(closed, old);
    }
    cerr << "publisher: closed\n";

    loop.shutdown();

    cerr << "publisher: final numDone = " + to_string(numDone) + "\n";

    cerr << "publisher: exit\n";
}

void
doSubscriberThread()
{
    MessageLoop loop;
    loop.start();

    int closed(true);
    auto onClosed = [&] (bool fromPeer,
                               const std::vector<std::string> & msgs) {
        cerr << "subscriber: disconnected\n";
        closed = true;
        ML::futex_wake(closed);
    };

    int numReceived(0);
    std::shared_ptr<NsqClient> client;
    auto onMessage = [&] (Date ts, const string & messageId,
                          const string & message) {
        numReceived++;
        client->fin(messageId);
    };

    client.reset(new NsqClient(onClosed, onMessage));
    loop.addSource("client", client);
    client->init("http://127.0.0.1:4150");
    cerr << "subscriber: connect...\n";
    auto result = client->connectSync();
    if (result != ConnectionResult::Success) {
        throw ML::Exception("connection error");
    }
    closed = false;

    auto onIdentify = [&] (const NsqFrame & response) {
        cerr << "subscriber: onIdentify subscriber\n";
        client->sub("a-topic", "a-channel");
    };
    cerr << "subscriber: identify...\n";
    client->identify(onIdentify);
    
    while (numReceived < numMessages) {
        cerr << "subscriber: numReceived = " + to_string(numReceived) + "\n";
        ML::sleep(1.0);
    }

    client->requestClose();

    cerr << "subscriber: waiting for close state...\n";
    while (!closed) {
        int old = closed;
        ML::futex_wait(closed, old);
    }
    cerr << "subscriber: closed\n";

    loop.shutdown();

    cerr << "subscriber: final numReceived = " + to_string(numReceived) + "\n";
    cerr << "subscriber: exit\n";
}


#if 1
BOOST_AUTO_TEST_CASE( test_http_client_get )
{
    vector<std::thread> threads;
    auto publisherThread = [&] () {
        doPublisherThread();
    };
    threads.emplace_back(publisherThread);
    auto subscriberThread = [&] () {
        doSubscriberThread();
    };
    threads.emplace_back(subscriberThread);
    for (auto & th: threads) {
        th.join();
    }
    cerr << "threads joined\n";
}
#endif
