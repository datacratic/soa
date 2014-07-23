"""
Takes a pid as a parameter. Reads /proc/<pid>/stat and returns it into JSON.
Field          Content
    pid           process id
    tcomm         filename of the executable
    state         state (R is running, S is sleeping, D is sleeping in an
                  uninterruptible wait, Z is zombie, T is traced or stopped)
    ppid          process id of the parent process
    pgrp          pgrp of the process
    sid           session id
    tty_nr        tty the process uses
    tty_pgrp      pgrp of the tty
    flags         task flags
    min_flt       number of minor faults
    cmin_flt      number of minor faults with child's
    maj_flt       number of major faults
    cmaj_flt      number of major faults with child's
    utime         user mode jiffies
    stime         kernel mode jiffies
    cutime        user mode jiffies with child's
    cstime        kernel mode jiffies with child's
"""
import sys
fields_but_pid = ["tcomm", "state", "ppid", "pgrp", "sid", "tty_nr",
                  "tty_pgrp", "flags", "min_flt", "cmin_flt", "maj_flt",
                  "cmaj_flt", "utime", "stime", "cutime", "cstime"]


def _getProcStatParts(pid):
    f = open("/proc/" + pid + "/stat", 'r')
    lines = f.read()
    f.close()
    return lines.split(" ")

def getProcStatJson(pid):
    parts = _getProcStatParts(pid)
    result = {"pid" : parts[0]}
    index = 1
    for k in fields_but_pid:
        result[k] = parts[index]
        index += 1
    return result

def getProcStatJsonStr(pid):
    parts = _getProcStatParts(pid)
    index = 1
    result = '{ "pid" :' + parts[0]
    for k in fields_but_pid:
        result += ', "' + k + '" : "' +  parts[index] + '"'
        index += 1
    return result + "}"

if __name__ == "__main__":
    if not len(sys.argv):
        print "You need to specify a PID"
        sys.exit(1)
    print getProcStatJsonStr(sys.argv[1])

