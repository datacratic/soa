/* runner_helper.cc
   Wolfgang Sourdeau, September 2013
   Copyright (c) 2013 Datacratic.  All rights reserved.

   A helper program that performs various process accounting tasks and reports
   the process status to the Runner.
*/

#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "jml/arch/exception.h"
#include "jml/utils/guard.h"

#include "runner_common.h"

using namespace std;
using namespace Datacratic;


void
runChild(char * execArgs[], int childLaunchStatusFd[], ProcessFds & fds)
{
    ::close(childLaunchStatusFd[0]);

    ::setsid();

    ::signal(SIGQUIT, SIG_DFL);
    ::signal(SIGTERM, SIG_DFL);
    ::signal(SIGINT, SIG_DFL);

    ::prctl(PR_SET_PDEATHSIG, SIGHUP);
    if (getppid() == 1) {
        ::fprintf(stderr, "runner: parent process already dead\n");
        ::kill(getpid(), SIGHUP);
    }
    ::close(fds.statusFd);

    // printf("runChild: ");
    // char **currentArg = execArgs;
    // while (*currentArg) {
    //     printf(" %s", *currentArg);
    //     currentArg++;
    // }
    // printf("\n");

    int res = ::execv(execArgs[0], execArgs);
    if (res == -1) {
        // Report back that we couldn't launch
        int err = errno;
        int res = ::write(childLaunchStatusFd[1], &err, sizeof(err));
        if (res == -1)
            _exit(124);
        else _exit(125);
    }
}

void
monitorChild(int childPid, int childLaunchStatusFd[], ProcessFds & fds)
{
    ::prctl(PR_SET_PDEATHSIG, SIGHUP);

    ::close(childLaunchStatusFd[1]);
    childLaunchStatusFd[1] = -1;
    // FILE * terminal = ::fopen("/dev/tty", "a");
    // ::fprintf(terminal, "wrapper: real child pid: %d\n", childPid);
    ProcessStatus status;

    // Write an update to the current status
    auto writeStatus = [&] () {
        int res = ::write(fds.statusFd, &status, sizeof(status));
        if (res == -1)
            throw ML::Exception(errno, "runWrapper write status");
        else if (res != sizeof(status))
            throw ML::Exception("didn't completely write status");
    };

    // Write that there was an error to the calling process, and then
    // exit
    auto writeError = [&] (int launchErrno, LaunchErrorCode errorCode,
                           int exitCode) {
        status.launchErrno = launchErrno;
        status.launchErrorCode = errorCode;
                
        //cerr << "sending error " << strerror(launchErrno)
        //<< " " << strLaunchError(errorCode) << " and exiting with "
        //<< exitCode << endl;

        int res = ::write(fds.statusFd, &status, sizeof(status));
        if (res == -1)
            throw ML::Exception(errno, "runWrapper write status");
        else if (res != sizeof(status))
            throw ML::Exception("didn't completely write status");

        fds.close();
                
        _exit(exitCode);
    };

    status.state = ST_LAUNCHING;
    status.pid = childPid;

    writeStatus();

    // ::fprintf(terminal, "wrapper: waiting child...\n");

    // Read from the launch status pipe to know that the launch has
    // finished.
    int launchErrno;
    int bytes = ::read(childLaunchStatusFd[0], &launchErrno,
                       sizeof(launchErrno));
        
    if (bytes == 0) {
        // Launch happened successfully (pipe was closed on exec)
        status.state = ST_RUNNING;
        writeStatus();
    }
    else {
        // Error launching

        //cerr << "got launch error" << endl;
        int childStatus;
        // We ignore the error code for this... there is nothing we
        // can do if we can't waitpid
        while (::waitpid(childPid, &childStatus, 0) == -1 && errno == EINTR) ;

        //cerr << "waitpid on " << childPid << " returned "
        //     << res << " with childStatus "
        //     << childStatus << endl;

        //cerr << "done with an error; first wait for the child to exit"
        //     << endl;

        if (bytes == -1) {
            // Problem reading
            writeError(errno, E_READ_STATUS_PIPE, 127);
        }
        else if (bytes != sizeof(launchErrno)) {
            // Wrong size of message
            writeError(0, E_STATUS_PIPE_WRONG_LENGTH, 127);
        }
        else {
            // Launch was unsuccessful; we have the errno.  Return it and
            // exit.
            writeError(launchErrno, E_SUBTASK_LAUNCH, 126);
        }
    }

    int childStatus;
    int res;
    while ((res = ::waitpid(childPid, &childStatus, 0)) == -1
           && errno == EINTR);
    if (res == -1) {
        writeError(errno, E_SUBTASK_WAITPID, 127);
    }
    else if (res != childPid) {
        writeError(0, E_WRONG_CHILD, 127);
    }

    status.state = ST_STOPPED;
    status.childStatus = childStatus;
    getrusage(RUSAGE_CHILDREN, &status.usage);

    writeStatus();

    fds.close();
}

int main(int argc, char * argv[])
{
    if (argc < 2) {
        ::fprintf(stderr, "missing argument\n");
        exit(-1);
    }

    // printf("helper: ");
    // for (int i = 0; i < argc; i++) {
    //     printf(" %d:%s", i, argv[i]);
    // }
    // printf("\n");

    // Undo any SIGCHLD block from the parent process so it can
    // properly wait for the signal
    ::signal(SIGCHLD, SIG_DFL);
    ::signal(SIGPIPE, SIG_DFL);

    ProcessFds fds;
    fds.decodeFromBuffer(argv[1]);
    fds.dupToStdStreams();
    fds.closeRemainingFds();

    char * execArgs[argc - 1];
    for (int i = 2; i < argc; i++) {
        execArgs[i - 2] = argv[i];
    }
    execArgs[argc - 2] = nullptr;

    // Create a pipe for the child to accurately report launch errors back
    // to the parent.  We set the close-on-exit so that when the new
    // process has finished launching, the pipe will be completely closed
    // and we can use this to know that it has properly started.

    int childLaunchStatusFd[2] = { -1, -1 };

    // Arrange for them to be closed in the case of an exception.
    ML::Call_Guard guard([&] () {
        if (childLaunchStatusFd[0] != -1)
            ::close(childLaunchStatusFd[0]);
        if (childLaunchStatusFd[1] != -1)
            ::close(childLaunchStatusFd[1]);
    });
    int res = ::pipe2(childLaunchStatusFd, O_CLOEXEC);
    if (res == -1)
        throw ML::Exception(errno, "pipe() for status");

    int childPid = fork();
    if (childPid == 0) {
        runChild(execArgs, childLaunchStatusFd, fds);
        /* there is no possible way this code could be executed, because
         * "runChild" calls "execv" */
        throw ML::Exception("The Alpha became the Omega.");
    }
    else if (childPid == -1) {
        throw ML::Exception(errno, "fork() in runWrapper");
    }
    else {
        monitorChild(childPid, childLaunchStatusFd, fds);
    }

    return 0;
}
