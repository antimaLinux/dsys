from __future__ import absolute_import, print_function, division, unicode_literals

from dsys.src.logging.dsys_logger_client import get_logger
from dsys.src.utils import registered_tasks
from functools import wraps
import time
import sys
import inspect

log = get_logger(__name__)


class TimeOutException(Exception):
    pass


class Task(object):

    registered_functions = dict(inspect.getmembers(registered_tasks, inspect.isfunction))
    functions_mappings = {v: k for k, v in registered_functions.items()}

    def __init__(self, id, func, args):
        self.id = str(id)
        self.args = args
        self.func = func

    def __call__(self):

        if isinstance(self.args, tuple) or isinstance(self.args, list):
            # time.sleep(0.1)
            return self.func(*self.args)
        elif isinstance(self.args, dict):
            # time.sleep(0.1)
            return self.func(**self.args)
        else:
            # time.sleep(0.1)
            return self.func(self.args)

    def __getstate__(self):
        log.info("Enqueued Task %s" % self.id)

        return {
            'id': self.id,
            'args': self.args,
            'func': self.func
        }

    def __setstate__(self, data):
        self.__dict__ = data

    @property
    def to_dict(self):
        if self.func not in self.functions_mappings:
            self.functions_mappings[self.func] = self.func.__name__
            self.registered_functions[self.func.__name__] = self.func

        return dict(id=self.id, args=self.args, func=self.functions_mappings[self.func])

    @classmethod
    def from_dict(cls, data):
        return cls(id=data['id'], args=data['args'], func=cls.registered_functions[data['func']])

    def __repr__(self):
        return "Task %s -- %s(%r)" % (self.id, self.__class__, self.__dict__)


def delay(wait=0.):
    def outer(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log.info("Waiting {} to execute".format(wait))
            time.sleep(wait)
            return func(*args, **kwargs)

        return wrapper

    return outer


def example_exc_handler(tries_remaining, exception, delay):
    print("Caught '%s', %d tries remaining, sleeping for %s seconds" % (exception, tries_remaining, delay),
          file=sys.stderr)


def dummy_function(*args, **kwargs):
    import math
    print("called with args: {} kwargs: {}".format(args, kwargs))
    return math.factorial(args[0])