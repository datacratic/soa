/* json_parsing_test.cc
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   Test for the environment functions.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "soa/types/json_parsing.h"
#include "soa/utf8cpp/source/utf8.h"
#include "jml/utils/info.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/auto_unit_test.hpp>
#include <math.h>

using namespace ML;
using namespace Datacratic;

using boost::unit_test::test_suite;

void testUnsigned(const std::string & str, unsigned long long expected)
{
    Parse_Context context(str, str.c_str(), str.c_str() + str.size());
    auto val = expectJsonNumber(context);
    context.expect_eof();
    BOOST_CHECK_EQUAL(val.uns, expected);
    BOOST_CHECK_EQUAL(val.type, JsonNumber::UNSIGNED_INT);
}

void testSigned(const std::string & str, long long expected)
{
    Parse_Context context(str, str.c_str(), str.c_str() + str.size());
    auto val = expectJsonNumber(context);
    context.expect_eof();
    BOOST_CHECK_EQUAL(val.uns, expected);
    BOOST_CHECK_EQUAL(val.type, JsonNumber::SIGNED_INT);
}

void testFp(const std::string & str, double expected)
{
    Parse_Context context(str, str.c_str(), str.c_str() + str.size());
    auto val = expectJsonNumber(context);
    context.expect_eof();
    BOOST_CHECK_EQUAL(val.fp, expected);
    BOOST_CHECK_EQUAL(val.type, JsonNumber::FLOATING_POINT);
}

void testHex4(const std::string & str, long long expected)
{
    Parse_Context context(str, str.c_str(), str.c_str() + str.size());
    auto val = context.expect_hex4();
    context.expect_eof();
    BOOST_CHECK_EQUAL(val, expected);
}

BOOST_AUTO_TEST_CASE( test1 )
{
    testUnsigned("0", 0);
    testSigned("-0", 0);
    testSigned("-1", -1);
    testFp("0.", 0.0);
    testFp(".1", 0.1);
    testFp("-.1", -0.1);
    testFp("0.0", 0.0);
    testFp("1e0", 1e0);
    testFp("-1e0", -1e0);
    testFp("-1e+0", -1e+0);
    testFp("-1e-0", -1e-0);
    testFp("-1E+3", -1e+3);
    testFp("1.0E-3", 1.0E-3);
    testFp("Inf", INFINITY);
    testFp("-Inf", -INFINITY);

    testHex4("0026", 38);
    testHex4("001A", 26);
    
    JML_TRACE_EXCEPTIONS(false);
    BOOST_CHECK_THROW(testFp(".", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("e3", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("3e", 0.1), std::exception);
    BOOST_CHECK_THROW(testFp("3.1aade", 0.1), std::exception);
    
    BOOST_CHECK_THROW(testHex4("002", 2), std::exception);
    BOOST_CHECK_THROW(testHex4("002G", 2), std::exception);
    BOOST_CHECK_THROW(testHex4("002.", 2), std::exception);
}

Parse_Context ctx(const std::string& str) {
    return Parse_Context(str, str.c_str(), str.c_str() + str.size());
}

static void title(std::string str) {
    std::cerr << str << std::endl;
    std::cerr << std::string(str.size(), '-') << std::endl;
}

static void checkThrow(std::string json) {
    Parse_Context context = ctx(json);

    Set_Trace_Exceptions guard(false);
    auto fn = [](const std::string& str, ML::Parse_Context& context) {
        expectJsonString(context);
    };
    BOOST_CHECK_THROW(expectJsonObject(context, fn), std::exception);
}

BOOST_AUTO_TEST_CASE ( test_unicode_parsing )
{

    title("Regular ascii");
    {
        std::string asciiJson = "{ \"foo\": \"bar\" }";

        Parse_Context context = ctx(asciiJson);
        expectJsonObject(context, [](const std::string& str, ML::Parse_Context& context) {
            BOOST_CHECK_EQUAL(str, "foo");
            BOOST_CHECK_EQUAL(expectJsonString(context), "bar");
        });
    }

    title("Valid UTF-8, smiley face");
    {
        std::string unicodeJson = "{ \"data\": \"\xE2\x98\xBA happy\" }";
        Parse_Context context = ctx(unicodeJson);
        expectJsonObject(context, [](const std::string& str, ML::Parse_Context& context) {
            BOOST_CHECK_EQUAL(str, "data");
            BOOST_CHECK_EQUAL(expectJsonString(context), "\xE2\x98\xBA happy");
        });
    }

    title("Invalid UTF-8, overlong sequence");
    /* Overlong dot encoded in two bytes */
    checkThrow("{ \"data\": \"\xC0\xAE\" }");

    title("Invalid UTF-8, one missing byte");
    /* Monkey face, last byte missing */
    checkThrow("{ \"data\": \"\xF0\x9F\x90\" }");

    title("Invalid UTF-8, invalid byte");
    /* Monkey face, invalid last byte */
    checkThrow("{ \"data\": \"\xF0\x9F\x90\x34\" }");
}
