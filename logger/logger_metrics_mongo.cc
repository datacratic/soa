/* logger_metrics_interface.cc
   François-Michel L'Heureux, 21 May 2013
   Copyright (c) 2013 Datacratic.  All rights reserved.
*/

#include "mongo/bson/bson.h"
#include "mongo/util/net/hostandport.h"
#include "jml/utils/string_functions.h"
#include "logger_metrics_mongo.h"
#include "soa/utils/mongo_init.h"


using namespace std;
using namespace mongo;
using namespace Datacratic;


/****************************************************************************/
/* LOGGER METRICS MONGO                                                     */
/****************************************************************************/

LoggerMetricsMongo::
LoggerMetricsMongo(Json::Value config, const string & coll,
                   const string & appName)
    : ILoggerMetrics(coll)
{
    for (const string & s: {"hostAndPort", "database", "user", "pwd"}) {
        if (config[s].isNull()) {
            throw ML::Exception("Missing LoggerMetricsMongo parameter [%s]",
                                s.c_str());
        }
    }

    vector<string> hapStrs = ML::split(config["hostAndPort"].asString(), ',');
    if (hapStrs.size() > 1) {
        vector<HostAndPort> haps;
        for (const string & hapStr: hapStrs) {
            haps.emplace_back(hapStr);
        }
        conn.reset(new mongo::DBClientReplicaSet(hapStrs[0], haps, 100));
    }
    else {
        auto tmpConn = make_shared<DBClientConnection>();
        tmpConn->connect(hapStrs[0]);
        conn = tmpConn;
    }
    db = config["database"].asString();

    auto impl = [&] (string mechanism) {
        BSONObj b = BSON("user" << config["user"].asString()
                  << "pwd" << config["pwd"].asString()
                  << "mechanism" << mechanism
                  << "db" << db);

        try {
            JML_TRACE_EXCEPTIONS(false);
            conn->auth(b);
        }
        catch (const UserException & _) {
            return false;
        }
        return true;
    };

    if (!impl("SCRAM-SHA-1")) {
        cerr << "Failed to authenticate with SCRAM-SHA-1, "
                "trying with MONGODB-CR" << endl;
        if (!impl("MONGODB-CR")) {
            cerr << "Failed with MONGODB-CR as well" << endl;
            throw ML::Exception("Failed to authenticate");
        }
    }

    BSONObj obj = BSON(GENOID);
    conn->insert(db + "." + coll, obj);
    objectId = obj["_id"].OID();
    logToTerm = config["logToTerm"].asBool();
}

void
LoggerMetricsMongo::
logInCategory(const string & category, const Json::Value & json)
{
    if (category.find(".") != std::string::npos) {
        throw ML::Exception("mongo does not support dotted keys:"
                            " \"%s\"", category.c_str());
    }

    vector<string> stack;
    function<BSONObj (const Json::Value &, const string & prefix)> buildObject;
    function<BSONArray (const Json::Value &)> buildArray;

    buildArray = [&] (const Json::Value & v) {
        BSONArrayBuilder arrBuilder;

        for (const auto & current: v) {
            if (current.isInt()) {
                arrBuilder.append(current.asInt());
            }
            else if (current.isUInt()) {
                arrBuilder.append((uint32_t)current.asUInt());
            }
            else if (current.isDouble()) {
                arrBuilder.append(current.asDouble());
            }
            else if (current.isObject()) {
                auto obj = buildObject(current, "");
                arrBuilder.append(obj);
            }
            else {
                arrBuilder.append(current.asString());
            }
        }

        return arrBuilder.arr();
    };

    buildObject = [&] (const Json::Value & v, const string & prefix) {
        BSONObjBuilder objBuilder;
        
        for (auto it = v.begin(); it != v.end(); ++it) {
            string memberName = it.memberName();
            if (memberName.find(".") != std::string::npos) {
                throw ML::Exception("mongo does not support dotted keys:"
                                    " \"%s\"", memberName.c_str());
            }

            const auto & current = *it;
            if (current.isObject()) {
                auto subObj = buildObject(current, "");
                objBuilder.append(prefix + memberName, subObj);
            }
            else if (current.isArray()) {
                auto arr = buildArray(current);
                objBuilder.append(prefix + memberName, arr);
            }
            else if (current.isInt()) {
                objBuilder.append(prefix + memberName, current.asInt());
            }
            else if (current.isUInt()) {
                objBuilder.append(prefix + memberName, (uint32_t)current.asUInt());
            }
            else if (current.isDouble()) {
                objBuilder.append(prefix + memberName, current.asDouble());
            }
            else {
                objBuilder.append(prefix + memberName, current.asString());
            }
        }

        return objBuilder.obj();
    };

    if (logToTerm) {
        cout << objectId << "." << coll << "." << category 
             << ": " << json.toStyledString() << endl;
    }

    auto obj = buildObject(json, category + ".");
    conn->update(db + "." + coll, BSON("_id" << objectId),
                 BSON("$set" << obj), true);
}

void
LoggerMetricsMongo::
logInCategory(const std::string & category,
              const std::vector<std::string> & path,
              const NumOrStr & val)
{
    if (path.empty()) {
        throw ML::Exception("You need to specify a path where to log"
                            " the value");
    }
    stringstream newCat;
    newCat << category;
    for (const string & part: path) {
        if (part.find(".") != std::string::npos) {
            throw ML::Exception("mongo does not support dotted keys,"
                    " hence \"%s\" is invalid", part.c_str());
        }
        newCat << "." << part;
    }
    string newCatStr = newCat.str();
    
    BSONObj bsonObj;
    //reference
    //typedef boost::variant<int, float, double, size_t, uint32_t, String> NumOrStr;
    int type = val.which();
    if (type == 0) {
        bsonObj = BSON(newCatStr << boost::get<int>(val));
    }
    else if (type == 1) {
        bsonObj = BSON(newCatStr << boost::get<float>(val));
    }
    else if (type == 2) {
        bsonObj = BSON(newCatStr << boost::get<double>(val));
    }
    else if (type == 3) {
        bsonObj = BSON(newCatStr << (int)boost::get<size_t>(val));
    }
    else if (type == 4) {
        bsonObj = BSON(newCatStr << boost::get<uint32_t>(val));
    }
    else {
        stringstream ss;
        ss << val;
        string str = ss.str();
        if (type != 5) {
            cerr << "Unknown type of NumOrStr for value: " << str << endl;
        }
        bsonObj = BSON(newCatStr << str);
    }
    if (logToTerm) {
        cerr << bsonObj.toString() << endl;
    }
    conn->update(db + "." + coll, BSON("_id" << objectId),
                 BSON("$set" << bsonObj), true);
}

std::string
LoggerMetricsMongo::
getProcessId()
    const
{
    return objectId.toString();
}
