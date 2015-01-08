/* event_service.cc
   Jeremy Barnes, 29 May 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   High frequency logging service.
*/

#include "service_base.h"
#include <iostream>
#include "soa/service/carbon_connector.h"
#include "zookeeper_configuration_service.h"
#include "jml/arch/demangle.h"
#include "jml/utils/exc_assert.h"
#include "jml/utils/environment.h"
#include "jml/utils/file_functions.h"
#include <unistd.h>
#include <sys/utsname.h>
#include <boost/make_shared.hpp>
#include "zmq.hpp"
#include "soa/jsoncpp/reader.h"
#include "soa/jsoncpp/value.h"
#include <fstream>
#include <sys/utsname.h>

using namespace std;

extern const char * __progname;

namespace Datacratic {

/*****************************************************************************/
/* EVENT SERVICE                                                             */
/*****************************************************************************/

std::map<std::string, double>
EventService::
get(std::ostream & output) const {
    std::map<std::string, double> result;

    std::stringstream ss;
    dump(ss);

    while (ss)
    {
        string line;
        getline(ss, line);
        if (line.empty()) continue;

        size_t pos = line.rfind(':');
        ExcAssertNotEqual(pos, string::npos);
        string key = line.substr(0, pos);

        pos = line.find_first_not_of(" \t", pos + 1);
        ExcAssertNotEqual(pos, string::npos);
        double value = stod(line.substr(pos));
        result[key] = value;
    }

    output << ss.str();
    return result;
}

/*****************************************************************************/
/* NULL EVENT SERVICE                                                        */
/*****************************************************************************/

NullEventService::
NullEventService()
    : stats(new MultiAggregator())
{
}

NullEventService::
~NullEventService()
{
}

void
NullEventService::
onEvent(const std::string & name,
        const char * event,
        EventType type,
        float value)
{
    stats->record(name + "." + event, type, value);
}

void
NullEventService::
dump(std::ostream & stream) const
{
    stats->dumpSync(stream);
}


/*****************************************************************************/
/* CARBON EVENT SERVICE                                                      */
/*****************************************************************************/

CarbonEventService::
CarbonEventService(std::shared_ptr<CarbonConnector> conn) :
    connector(conn)
{
}

CarbonEventService::
CarbonEventService(const std::string & carbonAddress,
                   const std::string & prefix,
                   double dumpInterval)
    : connector(new CarbonConnector(carbonAddress, prefix, dumpInterval))
{
}

CarbonEventService::
CarbonEventService(const std::vector<std::string> & carbonAddresses,
                   const std::string & prefix,
                   double dumpInterval)
    : connector(new CarbonConnector(carbonAddresses, prefix, dumpInterval))
{
}

void
CarbonEventService::
onEvent(const std::string & name,
        const char * event,
        EventType type,
        float value)
{
    if (name.empty()) {
        connector->record(event, type, value);
    }
    else {
        size_t l = strlen(event);
        string s;
        s.reserve(name.length() + l + 2);
        s.append(name);
        s.push_back('.');
        s.append(event, event + l);
        connector->record(s, type, value);
    }
}


/*****************************************************************************/
/* EVENT RECORDER                                                            */
/*****************************************************************************/

EventRecorder::
EventRecorder(const std::string & eventPrefix,
              const std::shared_ptr<EventService> & events)
    : eventPrefix_(eventPrefix),
      events_(events)
{
}

EventRecorder::
EventRecorder(const std::string & eventPrefix,
              const std::shared_ptr<ServiceProxies> & services)
    : eventPrefix_(eventPrefix),
      services_(services)
{
}

void
EventRecorder::
recordEvent(const char * eventName,
            EventType type,
            float value) const
{
    EventService * es = 0;
    if (events_)
        es = events_.get();
    if (!es && services_)
        es = services_->events.get();
    if (!es)
        {
            std::cerr << "no services configured!!!!" << std::endl;
            return;
        }
    es->onEvent(eventPrefix_, eventName, type, value);
}

void
EventRecorder::
recordEventFmt(EventType type,
               float value,
               const char * fmt, ...) const
{
    if (!events_ && (!services_ || !services_->events))  return;
        
    char buf[2048];
        
    va_list ap;
    va_start(ap, fmt);
    try {
        int res = vsnprintf(buf, 2048, fmt, ap);
        if (res < 0)
            throw ML::Exception("unable to record hit with fmt");
        if (res >= 2048)
            throw ML::Exception("key is too long");
            
        recordEvent(buf, type, value);
        va_end(ap);
        return;
    }
    catch (...) {
        va_end(ap);
        throw;
    }
}

} // namespace Datacratic
