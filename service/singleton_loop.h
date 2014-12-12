#include "soa/service/async_writer_source.h"
#include "soa/service/message_loop.h"


namespace Datacratic {

/****************************************************************************/
/* SINGLETON LOOP ADAPTOR                                                   */
/****************************************************************************/

struct SingletonLoopAdaptor : public AsyncWriterSource {
    SingletonLoopAdaptor();
    ~SingletonLoopAdaptor();

    void removeSource(AsyncEventSource & source);
    void addSource(AsyncEventSource & newSource);
};


/****************************************************************************/
/* SINGLETON LOOP                                                           */
/****************************************************************************/

/* A minimalist event loop, similar to MessageLoop, that does not own pointers
   to its event sources. This enables the writing of classes that requires a
   MessageLoop internally, but that hides this requirement from their
   interface. Singleton loops are, as their name implies, designed to be
   instantiated as singletons. Sources are then expected to be added within
   their constructor and removed from their destructor. */
struct SingletonLoop {
    SingletonLoop();
    ~SingletonLoop();

    void start();
    void shutdown();

    void addSource(AsyncEventSource & newSource);

    /* Remove a source from the interest list. This function guarantees:
       - that events being processed will not be interfered with
       - that no more events are going to be handled when it returns
    */
    void removeSource(AsyncEventSource & source);

private:
    bool started_;

    MessageLoop loop_;
    std::shared_ptr<SingletonLoopAdaptor> adaptor_;
};

} // namespace Datacratic
