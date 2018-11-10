from __future__ import absolute_import, print_function, division, unicode_literals

import time
from functools import wraps

from src.logging.dsys_logger_client import get_logger
import simplejson as json
import dill

log = get_logger(__name__)

_registered_functions = dict()


class Task(object):

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
        log.debug("Enqueued Task %s" % self.id)

        return {
            'id': self.id,
            'args': self.args,
            'func': dill.dumps(self.func)
        }

    def __setstate__(self, data):
        self.__dict__ = data
        self.func = dill.loads(self.func)
        log.debug("Selected Task %s" % self.id)

    @property
    def as_dict(self):
        return dict(id=self.id, args=self.args, func=self.func.__name__)

    def to_json(self):
        return json.dumps(self.as_dict)

    @classmethod
    def from_dict(cls, data, registered_functions):
        return cls(id=data['id'], args=data['args'], func=registered_functions[data['func']])

    def __repr__(self):
        return "Task %s -- %s(%r)" % (self.id, self.__class__, self.__dict__)


class BaseTaskWrap(object):
    def __init__(self, _function):
        self._function = _function
        self.__name__ = _function.__name__

    def __call__(self, *args, **kwargs):
        log.debug("Calling {} with params [{}] [{}]".format(self.__name__, args, kwargs))
        try:
            return self._function(*args, **kwargs)
        except Exception as e:
            log.error("Caught an exception in {} with params [{}] [{}]: {}".format(self.__name__,
                                                                                   args, kwargs,
                                                                                   str(e)))


class DelayedTaskWrap(object):
    def __init__(self, _function):
        self._function = _function
        self.__name__ = _function.__name__

    def __call__(self, *args, **kwargs):
        log.debug("Calling delayed {} with params [{}] [{}]".format(self.__name__, args, kwargs))
        try:
            return self._function(*args, **kwargs)
        except Exception as e:
            log.error("Caught an exception in {} with params [{}] [{}]: {}".format(self.__name__,
                                                                                   args, kwargs,
                                                                                   str(e)))


def task(name=None, container=_registered_functions):

    def inner(func):
        def wrapped(*args, **kwargs):
            return func(*args, **kwargs)

        wrapped.__name__ = func.__name__
        container[name if name else func.__name__] = BaseTaskWrap(wrapped)
        return wrapped

    return inner


def delayed_task(name=None, container=_registered_functions, wait=0.):

    def outer(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            log.debug("Waiting {} to execute".format(wait))
            time.sleep(wait)
            return func(*args, **kwargs)

        wrapped.__name__ = func.__name__
        container[name if name else func.__name__] = DelayedTaskWrap(wrapped)
        return wrapped

    return outer
