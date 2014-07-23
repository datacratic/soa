""" Utils to programmatically construct a Makefile in python.

    TODO : I did this script for a precise usecase, but thought it could be
    useful at some point, but it still needs to be added to build system.
"""

class MakeTarget(object):
    """ Makefile target """
    def __init__(self, targets, deps, cmd=None, aliases=None):

        if isinstance(targets, basestring):
            targets = [targets]
        if isinstance(deps, (basestring, MakeTarget)):
            deps = [deps]
        if isinstance(aliases, basestring):
            aliases = [aliases]

        self.targets = targets
        self.deps = []
        for d in deps:
            if isinstance(d, MakeTarget):
                for t in d.targets:
                    self.deps.append(t)
            else:
                self.deps.append(d)
        self.cmd = cmd
        self.aliases = [] if aliases is None else aliases

    def __str__(self):
        s = '{}: {}'.format(
            ' '.join(self.targets),
            ' '.join(self.deps))
        if self.cmd is not None:
            s += '\n\t' + self.cmd
        for alias in self.aliases:
            s += '\n{}: {}'.format(alias, self.targets[0])
        return s


class Makefile(object):
    """ Wrapper for a Makefile """
    def __init__(self):
        self.targets = []

    def add_target(self, target):
        self.targets.append(target)

    def add_targets(self, targets):
        self.targets.extend(targets)

    def __str__(self):
        return '\n\n'.join([str(t) for t in self.targets])

    def dump(self, file_object):
        file_object.write(str(self))
