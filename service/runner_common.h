/* runner_common.h                                                   -*-C++-*-
   Wolfgang Sourdeau, 10 December 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

#include <sys/time.h>
#include <sys/resource.h>

#include <string>


namespace Datacratic {

/****************************************************************************/
/* PROCESS FDS                                                              */
/****************************************************************************/

struct ProcessFds {
    ProcessFds();

    void closeRemainingFds();
    void dupToStdStreams();
    void close();

    int stdIn;
    int stdOut;
    int stdErr;
    int statusFd;
};


/****************************************************************************/
/* LAUNCH ERROR CODE                                                        */
/****************************************************************************/

/** Possible errors that could happen in launching.  These are
    enumerated here so that they can be passed back as an int
    rather than as a variable length string (or a const char *
    to memory which we could have to ensure was available in
    both the launcher process and the calling process).
*/
enum LaunchErrorCode {
    E_NONE,                     ///< No launch error
    E_READ_STATUS_PIPE,         ///< Error reading status pipe
    E_STATUS_PIPE_WRONG_LENGTH, ///< Status msg wrong length
    E_SUBTASK_LAUNCH,           ///< Error launching subtask
    E_SUBTASK_WAITPID,          ///< Error calling waitpid
    E_WRONG_CHILD               ///< Wrong child was reaped
};

/** Turn a launch error code into a descriptive string. */
std::string strLaunchError(LaunchErrorCode error);
            

/****************************************************************************/
/* PROCESS STATE                                                            */
/****************************************************************************/

/** State of the process. */
enum ProcessState {
    ST_UNKNOWN,    ///< Unknown status
    ST_LAUNCHING,     ///< Being launched
    ST_RUNNING,       ///< Currently running
    ST_STOPPED,       ///< No longer running
    ST_DONE           ///< Completely stopped
};

std::string statusStateAsString(ProcessState statusState);


/****************************************************************************/
/* PROCESS STATUS                                                           */
/****************************************************************************/

/** Structure passed back and forth between the launcher and the monitor to
    know the current state of the running process.
*/
struct ProcessStatus {
    ProcessStatus();

    ProcessState state;
    pid_t pid;
    int childStatus;
    int launchErrno;
    LaunchErrorCode launchErrorCode;
    rusage usage;
};

}
