#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <iostream>
#include <boost/test/unit_test.hpp>
#include "jml/utils/filter_streams.h"

using namespace std;
// using namespace Datacratic;

#if 1
BOOST_AUTO_TEST_CASE( test_hdfs_istream )
{
    string expected = "12345\ncoucou\n";
    ML::filter_istream stream("hdfs://localhost:9000/test-directory/test1");
    string content = stream.readAll();
    stream.close();

    BOOST_CHECK_EQUAL(content, expected);
}
#endif
