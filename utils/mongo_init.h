/**
 * mongo_init.h
 * Mich, 2015-09-03
 * Copyright (c) 2015 Datacratic. All rights reserved.
 **/
#pragma once

#include <vector>
#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"
#include "mongo/util/net/hostandport.h"


namespace Datacratic {

bool _mongoInitialized(false);

struct MongoAtInit {
    MongoAtInit()
    {
        if (!_mongoInitialized) {
            _mongoInitialized = true;
            using mongo::client::initialize;
            using mongo::client::Options;
            auto status = initialize();
            if (!status.isOK()) {
                throw ML::Exception("Mongo initialize failed");
            }
        }
    }
} atInit;

}
