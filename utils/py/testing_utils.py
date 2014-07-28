#
# testing_utils.py
# Simon Lemieux - 09 Jan 2013
# Copyright (c) 2013 Datacratic. All rights reserved.
#

# Ported from testing_utils.coffee

import os, atexit


class FileManager(object):

    def __init__(self, root='build/x86_64/tmp', dryrun=False):
        """
            Will delete the files/directories we create when exiting

            root    : where to put the files
            dryrun  : won't erase if set to True. Useful for debugging
        """
        self.stack = []
        self.root = root
        self.dryrun = dryrun
        if not os.path.exists(root):
            os.mkdir(root)
        atexit.register(self.clean)

    def register(self, path, use_root=True):
        """
            Add a path at `self.root`/`path` (`path` if `use_root`=False)
            that will be cleaned.
            Returns the actual path.
        """
        if use_root:
            path = os.path.join(self.root, path)
        self.stack.append(path)
        return path

    def createDir(self, path, use_root=True):
        """Create a directory and register it."""
        if use_root:
            path = os.path.join(self.root, path)
        if not os.path.exists(path):
            os.mkdir(path)
        self.register(path, False)
        return path

    def clean(self):
        """
        Cleans the registered files.
        You should not have to call it even though you still can.
        """
        while len(self.stack) > 0:
            path = self.stack.pop()
            if os.path.exists(path):
                if self.dryrun:
                    print 'Keeping', path
                    continue
                if os.path.isfile(path):
                    os.remove(path)
                    print 'Cleaned test file', path
                elif os.path.isdir(path):
                    os.rmdir(path)
                    print 'Cleaned test directory', path
                else:
                    raise ValueError('Did not delete "{}" because it is neither a file nor a directory')
