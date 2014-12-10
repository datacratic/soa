/* runner_common.cc
   Wolfgang Sourdeau, 10 December 2014
   Copyright (c) 2014 Datacratic.  All rights reserved.
*/

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "jml/arch/exception.h"

#include "runner_common.h"


using namespace std;
using namespace Datacratic;


namespace Datacratic {

std::string
strLaunchError(LaunchErrorCode error)
{
    switch (error) {
    case E_NONE: return "no error";
    case E_READ_STATUS_PIPE: return "read() on status pipe";
    case E_STATUS_PIPE_WRONG_LENGTH:
        return "wrong message size reading launch pipe";
    case E_SUBTASK_LAUNCH: return "exec() launching subtask";
    case E_SUBTASK_WAITPID: return "waitpid waiting for subtask";
    case E_WRONG_CHILD: return "waitpid() returned the wrong child";
    }
    throw ML::Exception("unknown error launch error code %d",
                        error);
}

std::string
statusStateAsString(ProcessState statusState)
{
    switch (statusState) {
    case ST_UNKNOWN: return "UNKNOWN";
    case ST_LAUNCHING: return "LAUNCHING";
    case ST_RUNNING: return "RUNNING";
    case ST_STOPPED: return "STOPPED";
    case ST_DONE: return "DONE";
    }
    throw ML::Exception("unknown status %d", statusState);
}

}


/****************************************************************************/
/* PROCESS FDS                                                              */
/****************************************************************************/

ProcessFds::
ProcessFds()
    : stdIn(::fileno(stdin)),
      stdOut(::fileno(stdout)),
      stdErr(::fileno(stderr)),
      statusFd(-1)
{
}

/* child api */
void
ProcessFds::
closeRemainingFds()
{
    struct rlimit limits;
    ::getrlimit(RLIMIT_NOFILE, &limits);

    for (int fd = 0; fd < limits.rlim_cur; fd++) {
        if ((fd != STDIN_FILENO || stdIn == -1)
            && fd != STDOUT_FILENO && fd != STDERR_FILENO
            && fd != statusFd) {
            ::close(fd);
        }
    }
}

void
ProcessFds::
dupToStdStreams()
{
    auto dupToStdStream = [&] (int oldFd, int newFd) {
        if (oldFd != newFd) {
            int rc = ::dup2(oldFd, newFd);
            if (rc == -1) {
                throw ML::Exception(errno,
                                    "ProcessFds::dupToStdStream dup2");
            }
        }
    };
    if (stdIn != -1) {
        dupToStdStream(stdIn, STDIN_FILENO);
    }
    dupToStdStream(stdOut, STDOUT_FILENO);
    dupToStdStream(stdErr, STDERR_FILENO);
}

/* parent & child api */
void
ProcessFds::
close()
{
    auto closeIfNotEqual = [&] (int & fd, int notValue) {
        if (fd != notValue) {
            ::close(fd);
        }
    };
    closeIfNotEqual(stdIn, STDIN_FILENO);
    closeIfNotEqual(stdOut, STDOUT_FILENO);
    closeIfNotEqual(stdErr, STDERR_FILENO);
    closeIfNotEqual(statusFd, -1);
}


/****************************************************************************/
/* PROCESS STATUS                                                           */
/****************************************************************************/

ProcessStatus::
ProcessStatus()
{
    // Doing it this way keeps ValGrind happy
    ::memset(this, 0, sizeof(*this));

    state = ST_UNKNOWN;
    pid = -1;
    childStatus = -1;
    launchErrno = 0;
    launchErrorCode = E_NONE;
}
