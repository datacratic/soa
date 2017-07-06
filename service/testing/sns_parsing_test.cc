#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string>

#include <boost/test/unit_test.hpp>
#include "jml/utils/filter_streams.h"
#include "soa/types/value_description.h"
#include "soa/service/sqs.h"

using namespace std;
using namespace Datacratic;


BOOST_AUTO_TEST_CASE( test_decode_SqsMessage )
{
    ML::filter_istream stream("soa/service/testing/sns-message.json");
    string msgStr = stream.readAll();
    stream.close();
    jsonDecodeStr<SqsApi::SnsMessageBody>(msgStr);
}
