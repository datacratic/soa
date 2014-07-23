# Some python utils

import os.path
import logging
import warnings

def ask_before_replace(file, msg='The file "%(file)s" will be replaced.'):
    """ Doesn't actually replace the file, just ask for confirmation.
        Return true if the users agrees with replacing the file, false otherwise.
    """
    if not os.path.exists(file):
        return True

    print msg % {'file': file}
    choice = ''
    while choice not in ['yes', 'no']:
        choice = raw_input('Proceed? (yes/no) ')
    if choice == 'yes':
        return True
    else:  # if 'no'
        return False


def configure_default_logging(logger_name, logging_level, logger_file):
    warnings.simplefilter('default') # enables DeprectionWarning
    log = logging.getLogger(logger_name)
    log.setLevel(logging_level)
    stream_handler = logging.StreamHandler()
    log.addHandler(stream_handler)

    if logger_file:
        file_handler = logging.handlers.RotatingFileHandler(
            logger_file,
            maxBytes=1024 * 1024, # 1MB
            backupCount=15) # 30 days
        log.addHandler(file_handler)


def deprecated(func):
    '''
    This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.
    '''
    def new_func(*args, **kwargs):
        warnings.warn("Call to deprecated function {}. See code to how to "
                      "replace it.".format(func.__name__),
                      category=DeprecationWarning)
        return func(*args, **kwargs)
    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func
