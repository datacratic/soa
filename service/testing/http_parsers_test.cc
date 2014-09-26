#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <boost/test/unit_test.hpp>

#include "soa/service/http_parsers.h"

using namespace std;
using namespace Datacratic;


#if 1
/* This test does progressive testing of the HttpResponseParser by sending
 * only a certain amount of bytes to check for all potential parsing faults,
 * for each step (top line, headers, body, response restart, ...). */
BOOST_AUTO_TEST_CASE( http_response_parser_test )
{
    string statusLine;
    vector<string> headers;
    string body;
    bool done;
    bool shouldClose;

    HttpResponseParser parser;
    parser.onResponseStart = [&] (const string & httpVersion, int code) {
        cerr << "response start\n";
        statusLine = httpVersion + "/" + to_string(code);
        headers.clear();
        body.clear();
        shouldClose = false;
        done = false;
    };
    parser.onHeader = [&] (const char * data, size_t size) {
        // cerr << "header: " + string(data, size) + "\n";
        headers.emplace_back(data, size);
    };
    parser.onData = [&] (const char * data, size_t size) {
        // cerr << "data\n";
        body.append(data, size);
    };
    parser.onDone = [&] (bool doClose) {
        shouldClose = doClose;
        done = true;
    };

    /* status line */
    parser.feed("HTTP/1.");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("1 200 Th");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("is is ");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("some blabla\r");
    BOOST_CHECK_EQUAL(statusLine, "");
    parser.feed("\n");
    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/200");

    /* headers */
    parser.feed("Head");
    BOOST_CHECK_EQUAL(headers.size(), 0);
    parser.feed("er1: value1\r\nHeader2: value2");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "Header1: value1");
    parser.feed("\r");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    parser.feed("\n");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    BOOST_CHECK_EQUAL(headers[1], "Header2: value2");
    parser.feed("");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    parser.feed("He");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    parser.feed("ad");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    parser.feed("er3: Val3\r\n");
    BOOST_CHECK_EQUAL(headers.size(), 3);
    BOOST_CHECK_EQUAL(headers[2], "Header3: Val3");
    parser.feed("Content-Length: 10\r\n\r");
    parser.feed("\n");
    BOOST_CHECK_EQUAL(headers.size(), 4);
    BOOST_CHECK_EQUAL(headers[3], "Content-Length: 10");
    BOOST_CHECK_EQUAL(parser.remainingBody(), 10);

    /* body */
    parser.feed("0123");
    parser.feed("456");
    parser.feed("789");
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);

    /* one full response and a partial one without body */
    parser.feed("HTTP/1.1 204 No content\r\n"
                "MyHeader: my value1\r\n\r\nHTTP");

    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/204");
    BOOST_CHECK_EQUAL(headers.size(), 1);
    BOOST_CHECK_EQUAL(headers[0], "MyHeader: my value1");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);

    parser.feed("/1.1 666 The number of the beast\r\n"
                "Connection: close\r\n"
                "Header: value\r\n\r\n");
    BOOST_CHECK_EQUAL(statusLine, "HTTP/1.1/666");
    BOOST_CHECK_EQUAL(headers.size(), 2);
    BOOST_CHECK_EQUAL(headers[0], "Connection: close");
    BOOST_CHECK_EQUAL(headers[1], "Header: value");
    BOOST_CHECK_EQUAL(body, "");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(shouldClose, true);
    BOOST_CHECK_EQUAL(parser.remainingBody(), 0);

    /* 2 full reponses with body */
    const char * payload = ("HTTP/1.1 200 This is some blabla\r\n"
                            "Header1: value1\r\n"
                            "Header2: value2\r\n"
                            "Content-Type: text/plain\r\n"
                            "Content-Length: 10\r\n"
                            "\r\n"
                            "0123456789");
    parser.feed(payload);
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);
    parser.feed(payload);
    BOOST_CHECK_EQUAL(body, "0123456789");
    BOOST_CHECK_EQUAL(done, true);
    BOOST_CHECK_EQUAL(shouldClose, false);
}
#endif
