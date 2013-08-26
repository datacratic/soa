/* gc_lock.h                                                       -*- C++ -*-
   Jeremy Barnes, 19 November 2011
   Copyright (c) 2011 Datacratic.  All rights reserved.

   "Lock" that works by deferring the destruction of objects into a garbage collection
   process which is only run when nothing could be using them.
*/

#ifndef __mmap__gc_lock_h__
#define __mmap__gc_lock_h__

#define GC_LOCK_DEBUG 0
#define GC_LOCK_SPIN_DEBUG 0

#include "jml/utils/exc_assert.h"
#include "jml/utils/abort.h"
#include "jml/arch/atomic_ops.h"
#include "jml/arch/thread_specific.h"
#include <vector>
#include <iostream>

#if GC_LOCK_DEBUG
#  include <iostream>
#endif

#include <chrono>

#if GC_LOCK_SPIN_DEBUG

#   define GCLOCK_SPINCHECK_DECL \
      std::chrono::time_point<std::chrono::system_clock> __start, __end;  \
      __start = std::chrono::system_clock::now();

#    define GCLOCK_SPINCHECK                                              \
       do {                                                               \
           __end = std::chrono::system_clock::now();                      \
           std::chrono::duration<double> elapsed = __end - __start;       \
           if (elapsed > std::chrono::seconds(10)) {                      \
               ML::do_abort();                                            \
               throw ML::Exception("GCLOCK_SPINCHECK: spent more than 10" \
                                   " seconds spinning");                  \
           }                                                              \
       } while (0)
#else
#   define GCLOCK_SPINCHECK_DECL
#   define GCLOCK_SPINCHECK ((void) 0)
#endif


namespace Datacratic {

/*****************************************************************************/
/* GC LOCK BASE                                                              */
/*****************************************************************************/

struct GcLockBase : public boost::noncopyable {

public:

    /** Enum for type safe specification of whether or not we run deferrals on
        entry or exit to a critical sections.  Thoss places that are latency
        sensitive should use RD_NO.
    */
    enum RunDefer {
        RD_NO = 0,      ///< Don't run deferred work on this call
        RD_YES = 1      ///< Potentially run deferred work on this call
    };

    enum DoLock {
        DONT_LOCK = 0,
        DO_LOCK = 1
    };

    /// A thread's bookkeeping info about each GC area
    struct ThreadGcInfoEntry {
        ThreadGcInfoEntry()
            : inEpoch(-1), readLocked(0), writeLocked(0), exclusiveLocked(0),
              writeEntered(0),
              owner(0)
        {
        }

        ~ThreadGcInfoEntry() {
            using namespace std;

            if (readLocked || writeLocked)
               cerr << "Thread died but GcLock is still locked" << endl;
           
        } 


        int inEpoch;  // 0, 1, -1 = not in 
        int readLocked;
        int writeLocked;
        int exclusiveLocked;
        int writeEntered;

        GcLockBase *owner;

        void init(const GcLockBase * const self) {
            if (!owner) 
                owner = const_cast<GcLockBase *>(self);
        }

        void enterShared(RunDefer runDefer) {
            if (!readLocked && !exclusiveLocked)
                owner->enterCS(this, runDefer);

            ++readLocked;
        }

        void exitShared(RunDefer runDefer) {
            if (readLocked <= 0)
                throw ML::Exception("Bad read lock nesting");

            --readLocked;
            if (!readLocked && !exclusiveLocked) 
                owner->exitCS(this, runDefer);

        }

        bool isLockedShared() {
            return readLocked + exclusiveLocked;
        }

        void lockExclusive() {
            if (readLocked || writeLocked) 
                throw ML::Exception("Can not lock exclusive while holding "
                                     "read or write lock");
            if (!exclusiveLocked)
                owner->enterCSExclusive(this);
            
             ++exclusiveLocked;
        }

        void unlockExclusive() {
            if (exclusiveLocked <= 0)
                throw ML::Exception("Bad exclusive lock nesting");

            --exclusiveLocked;
            if (!exclusiveLocked)
                owner->exitCSExclusive(this);
        }

        std::string print() const;

    };


    typedef ML::ThreadSpecificInstanceInfo<ThreadGcInfoEntry, GcLockBase>
        GcInfo;
    typedef typename GcInfo::PerThreadInfo ThreadGcInfo;

    typedef uint64_t Word;
    static constexpr size_t Bits = sizeof(Word) * CHAR_BIT;
    static constexpr Word StopBitMask = (1ULL << (Bits - 1));

    struct Data {
        Data();
        Data(const Data & other);

        Data & operator = (const Data & other);

        typedef uint64_t q2 __attribute__((__vector_size__(16)));
        
        volatile union {
            struct {
                int32_t epoch;       ///< Current epoch number (could be smaller).
                int16_t in[2];       ///< How many threads in each epoch
                int32_t visibleEpoch;///< Lowest epoch number that's visible
                int32_t exclusive;       ///< Mutex value for exclusive lock
            };
            struct {
                uint64_t bits;
                uint64_t bits2;
            };
            struct {
                q2 q;
            };
        } JML_ALIGNED(16);

        volatile Word writeLock;

        int16_t inCurrent() const { return in[epoch & 1]; }
        int16_t inOld() const { return in[(epoch - 1)&1]; }

        void setIn(int32_t epoch, int val)
        {
            //if (epoch != this->epoch && epoch + 1 != this->epoch)
            //    throw ML::Exception("modifying wrong epoch");
            in[epoch & 1] = val;
        }

        void addIn(int32_t epoch, int val)
        {
            //if (epoch != this->epoch && epoch + 1 != this->epoch)
            //    throw ML::Exception("modifying wrong epoch");
            in[epoch & 1] += val;
        }

        /** Check that the invariants all hold.  Throws an exception if not. */
        void validate() const;

        /** Calculate the appropriate value of visibleEpoch from the rest
            of the fields.  Returns true if waiters should be woken up.
        */
        bool calcVisibleEpoch();
        
        /** Human readable string. */
        std::string print() const;

        bool operator == (const Data & other) const
        {
            return bits == other.bits && bits2 == other.bits2;
        }

        bool operator != (const Data & other) const
        {
            return ! operator == (other);
        }

    } JML_ALIGNED(16);

    void enterCS(ThreadGcInfoEntry * entry = 0, RunDefer runDefer = RD_YES);
    void exitCS(ThreadGcInfoEntry * entry = 0, RunDefer runDefer = RD_YES);
    void enterCSExclusive(ThreadGcInfoEntry * entry = 0);
    void exitCSExclusive(ThreadGcInfoEntry * entry = 0);

    int myEpoch(GcInfo::PerThreadInfo * threadInfo = 0) const
    {
        return getEntry(threadInfo).inEpoch;
    }

    int currentEpoch() const
    {
        return data->epoch;
    }

    JML_ALWAYS_INLINE ThreadGcInfoEntry &
    getEntry(GcInfo::PerThreadInfo * info = 0) const
    {
        ThreadGcInfoEntry *entry = gcInfo.get(info);
        entry->init(this);
        return *entry;
    }

    GcLockBase();

    virtual ~GcLockBase();

    /** Permanently deletes any resources associated with this lock. */
    virtual void unlink() = 0;

    void enterShared(GcInfo::PerThreadInfo * info = 0,
                    RunDefer runDefer = RD_YES)
    {
        ThreadGcInfoEntry & entry = getEntry(info);

        entry.enterShared(runDefer);

#if GC_LOCK_DEBUG
        using namespace std;
        cerr << "enterShared "
             << this << " index " << index
             << ": now " << entry.print() << " data "
             << data->print() << endl;
#endif
    }

    void exitShared(GcInfo::PerThreadInfo * info = 0, 
                    RunDefer runDefer = RD_YES)
    {
        ThreadGcInfoEntry & entry = getEntry(info);

        entry.exitShared(runDefer);

#if GC_LOCK_DEBUG
        using namespace std;
        cerr << "exitShared "
             << this << " index " << index
             << ": now " << entry.print() << " data "
             << data->print() << endl;
#endif
    }

    void enterWriteShared(GcInfo::PerThreadInfo * info = 0,
                          RunDefer runDefer = RD_YES)
    {
        ThreadGcInfoEntry & entry = getEntry(info);
        if (!entry.writeEntered) {
            Word oldVal, newVal;
            GCLOCK_SPINCHECK_DECL

            for (;;) {
                auto writeLock = data->writeLock;
                // We are write locked, continue spinning
                GCLOCK_SPINCHECK;
                    
                if (writeLock & StopBitMask) {
                    sched_yield();
                    continue;
                }

                oldVal = data->writeLock;

                // When our thread reads current writeLock, it might
                // have been locked by an other thread spinning
                // right after having been unlocked.
                // Thus we could end up in a really weird situation,
                // where the linearization point marked by the CAS
                // would only be reached after the writeBarrier
                // in the locking thread.
                //
                // To prevent this, we check again if the stop bit
                // is set.
                if (oldVal & StopBitMask) {
                    sched_yield();
                    continue;
                }

                newVal = oldVal + 1;


                // If the CAS fails, someone else in the meantime
                // must have entered a CS and incremented the counter
                // behind our back. We then retry
                if (ML::cmp_xchg(data->writeLock, oldVal, newVal))
                    break;
            }
                    

            enterShared(info, runDefer);
        }
        ++entry.writeEntered;
    }

    void exitWriteShared(GcInfo::PerThreadInfo * info = 0,
                         RunDefer runDefer = RD_YES)
    {
        ThreadGcInfoEntry & entry = getEntry(info);
        ExcAssertGreater(entry.writeEntered, 0);
        --entry.writeEntered;
        if (!entry.writeEntered) {
            ExcAssertGreater(data->writeLock, 0);
            ML::atomic_dec(data->writeLock);

            exitShared(info, runDefer);
        }
    }

    int isLockedShared(GcInfo::PerThreadInfo * info = 0) const
    {
        ThreadGcInfoEntry & entry = getEntry(info);

        return entry.isLockedShared() || isLockedWrite();
    }

    bool isLockedWrite() const {
        return getEntry().writeLocked;
    }

    int lockedInEpoch(GcInfo::PerThreadInfo * info = 0) const
    {
        ThreadGcInfoEntry & entry = getEntry(info);

        return entry.inEpoch;
    }

    void lockExclusive(GcInfo::PerThreadInfo * info = 0)
    {
        ThreadGcInfoEntry & entry = getEntry(info);

        entry.lockExclusive();
#if GC_LOCK_DEBUG
        using namespace std;
        cerr << "lockExclusive "
             << this << " index " << index
             << ": now " << entry.print() << " data "
             << data->print() << endl;
#endif
    }

    void unlockExclusive(GcInfo::PerThreadInfo * info = 0)
    {
        ThreadGcInfoEntry & entry = getEntry(info);

        entry.unlockExclusive();

#if GC_LOCK_DEBUG
        using namespace std;
        cerr << "unlockExclusive"
             << this << " index " << index
             << ": now " << entry.print()
             << " data " << data->print() << endl;
#endif
    }

    int isLockedExclusive(GcInfo::PerThreadInfo * info = 0) const
    {
        ThreadGcInfoEntry & entry = getEntry(info);

        return entry.exclusiveLocked;
    }

    void lockWrite()
    {
        ThreadGcInfoEntry &entry = getEntry();
        if (!entry.writeLocked) {
            Word oldValue, newValue;
            GCLOCK_SPINCHECK_DECL
            for (;;) {
                GCLOCK_SPINCHECK;
                if (data->writeLock & StopBitMask)
                    continue;

                oldValue = data->writeLock;

                // See enterWriteShared for the reason of this
                // double-check.
                // TODO: is this really needed ?
                if (oldValue & StopBitMask)
                    continue;

                newValue = oldValue | StopBitMask;
                if (ML::cmp_xchg(data->writeLock, oldValue, newValue))
                    break;

            }

            // Stop bit must be set
            ExcAssertEqual((data->writeLock & StopBitMask), StopBitMask);

            // At this point, we stoped all the upcoming writes. However,
            // ongoing writes might still be executing. Issuing a writeBarrier
            // will wait for all writes to finish before continuing
            writeBarrier();

            // No writes must be ongoing
            ExcAssertEqual((data->writeLock & ~StopBitMask), 0); 
        }
        ++entry.writeLocked;
    }

    void writeBarrier() {
        // Busy-waiting for all writes to finish
        while ((data->writeLock & ~StopBitMask) > 0) { }
    }


    void unlockWrite(GcInfo::PerThreadInfo * info = 0)
    {
        ThreadGcInfoEntry &entry = getEntry();
        --entry.writeLocked;
        if (!entry.writeLocked) {
            Word oldValue = data->writeLock;
            Word newValue = oldValue & ~StopBitMask;
            if (!ML::cmp_xchg(data->writeLock, oldValue, newValue)) {
            }

        }
    }


    struct SharedGuard {
        SharedGuard(GcLockBase & lock,
                    RunDefer runDefer = RD_YES,
                    DoLock doLock = DO_LOCK)
            : lock(lock),
              runDefer_(runDefer),
              doLock_(doLock)
        {
            if (doLock_)
                lock.enterShared(0, runDefer_);
        }

        ~SharedGuard()
        {
            if (doLock_)
            lock.exitShared(0, runDefer_);
        }
        
        GcLockBase & lock;
        const RunDefer runDefer_;  ///< Can this do deferred work?
        const DoLock doLock_;      ///< Do we really lock?
    };


    struct WriteSharedGuard {
        WriteSharedGuard(GcLockBase & lock, RunDefer runDefer = RD_YES) 
            : lock(lock),
              runDefer(runDefer)
        {
            lock.enterWriteShared(0, runDefer);
        }

        ~WriteSharedGuard()
        {
            lock.exitWriteShared(0, runDefer);
        }

        GcLockBase & lock;
        const RunDefer runDefer;
    };

    struct WriteLockGuard {
        WriteLockGuard(GcLockBase & lock)
            : lock(lock)
        {
            lock.lockWrite();
        }

        ~WriteLockGuard()
        {
            lock.unlockWrite();
        }

        GcLockBase & lock;
    };

    struct ExclusiveGuard {
        ExclusiveGuard(GcLockBase & lock)
            : lock(lock)
        {
            lock.lockExclusive();
        }

        ~ExclusiveGuard()
        {
            lock.unlockExclusive();
        }

        GcLockBase & lock;
    };

    /** Wait until everything that's currently visible is no longer
        accessible.
        
        You can't call this if a guard is held, as it would deadlock waiting
        for itself to exit from the critical section.
    */
    void visibleBarrier();

    /** Wait until all defer functions that have been registered have been
        run.
    
        You can't call this if a guard is held, as it would deadlock waiting
        for itself to exit from the critical section.
    */
    void deferBarrier();

    void defer(boost::function<void ()> work);

    typedef void (WorkFn1) (void *);
    typedef void (WorkFn2) (void *, void *);
    typedef void (WorkFn3) (void *, void *, void *);

    void defer(void (work) (void *), void * arg);
    void defer(void (work) (void *, void *), void * arg1, void * arg2);
    void defer(void (work) (void *, void *, void *), void * arg1, void * arg2, void * arg3);

    template<typename T>
    void defer(void (*work) (T *), T * arg)
    {
        defer((WorkFn1 *)work, (void *)arg);
    }

    template<typename T>
    static void doDelete(T * arg)
    {
        delete arg;
    }

    template<typename T>
    void deferDelete(T * toDelete)
    {
        if (!toDelete) return;
        defer(doDelete<T>, toDelete);
    }

    template<typename... Args>
    void doDefer(void (fn) (Args...), Args...);

    template<typename Fn, typename... Args>
    void deferBind(Fn fn, Args... args)
    {
        boost::function<void ()> bound = boost::bind<void>(fn, args...);
        this->defer(bound);
    }

    void dump();

protected:
    Data* data;

private:
    struct Deferred;
    struct DeferredList;

    GcInfo gcInfo;


    Deferred * deferred;   ///< Deferred workloads (hidden structure)

    /** Update with the new value after first checking that the current
        value is the same as the old value.  Returns true if it
        succeeded; otherwise oldValue is updated with the new old
        value.

        As part of doing this, it will calculate the correct value for
        visibleEpoch() and, if it has changed, wake up anything waiting
        on that value, and will run any deferred handlers registered for
        that value.
    */
    bool updateData(Data & oldValue, Data & newValue, RunDefer runDefer);

    /** Executes any available deferred work. */
    void runDefers();

    /** Check what deferred updates need to be run and do them.  Must be
        called with deferred locked.
    */
    std::vector<DeferredList *> checkDefers();
};


/*****************************************************************************/
/* GC LOCK                                                                   */
/*****************************************************************************/

/** GcLock for use within a single process. */

struct GcLock : public GcLockBase
{
    GcLock();
    virtual ~GcLock();

    virtual void unlink();

private:

    Data localData;

};


/*****************************************************************************/
/* SHARED GC LOCK                                                            */
/*****************************************************************************/

/** Constants that can be used to control how resources are opened.
    Note that these are structs so we can more easily overload constructors.
*/
extern struct GcCreate {} GC_CREATE; ///< Open and initialize a new resource.
extern struct GcOpen {} GC_OPEN;     ///< Open an existing resource.


/** GcLock to be shared among multiple processes. */

struct SharedGcLock : public GcLockBase
{
    SharedGcLock(GcCreate, const std::string& name);
    SharedGcLock(GcOpen, const std::string& name);
    virtual ~SharedGcLock();

    /** Permanently deletes any resources associated with the gc lock. */
    virtual void unlink();

private:

    /** mmap an shm file into memory and set the data member of GcLock. */
    void doOpen(bool create);

    std::string name;
    int fd;
    void* addr;

};

} // namespace Datacratic


#endif /* __mmap__gc_lock_h__ */

