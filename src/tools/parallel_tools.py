from __future__ import absolute_import, print_function, division, unicode_literals
from Queue import Queue
from threading import Thread
from multiprocessing import Process, Pool
from contextlib import contextmanager
from functools import wraps
import collections
import time

import logging

log = logging.getLogger(__name__)


class asynchronous(object):
    """
    https://wiki.python.org/moin/PythonDecoratorLibrary#Asynchronous_Call
    ADDED wait_job_completion logic to Result class
    ADDED join method on Result Class
    FIXED queue.get is now parametrized and not blocking
    Case test:
        exception in thread ===> no data in queue ==> blocked

    """

    def __init__(self, func):
        self.func = func

        def threaded(*args, **kwargs):
            self.queue.put(self.func(*args, **kwargs))

        self.threaded = threaded

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def start(self, *args, **kwargs):
        self.queue = Queue()
        thread = Thread(target=self.threaded, args=args, kwargs=kwargs)
        thread.start()
        return asynchronous.Result(self.queue, thread)

    class NotYetDoneException(Exception):
        def __init__(self, message):
            self.message = message

    class Result(object):
        def __init__(self, queue, thread):
            self.queue = queue
            self.thread = thread

        def join(self):
            self.thread.join()

        def is_done(self):
            return not self.thread.is_alive()

        def wait_job_completion(self, delay=1):
            while True:
                if self.is_done():
                    break
                else:
                    print("Task still running, retrying in {} sec".format(delay))
                    time.sleep(delay)

        def get_result(self, block=False, timeout=None):
            if not self.is_done():
                raise asynchronous.NotYetDoneException('the call has not yet completed its task')

            if not hasattr(self, 'result'):
                self.result = self.queue.get(block=block, timeout=timeout)

            return self.result


class parallel(object):
    """
    Same as above
    Spawns processes instead of threads
    """

    def __init__(self, func):
        self.func = func

        def _parallel(*args, **kwargs):
            self.queue.put(self.func(*args, **kwargs))

        self.parallel_task = _parallel

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def start(self, *args, **kwargs):
        self.queue = Queue()
        proc = Process(target=self.parallel_task, args=args, kwargs=kwargs)
        proc.start()
        return parallel.Result(self.queue, proc)

    class NotYetDoneException(Exception):
        def __init__(self, message):
            self.message = message

    class Result(object):
        def __init__(self, queue, thread):
            self.queue = queue
            self.thread = thread

        def join(self):
            self.thread.join()

        def is_done(self):
            return not self.thread.is_alive()

        def wait_job_completion(self, delay=1):
            while True:
                if self.is_done():
                    break
                else:
                    print("Task still running, retrying in {} sec".format(delay))
                    time.sleep(delay)

        def get_result(self, block=False, timeout=None):
            if not self.is_done():
                raise parallel.NotYetDoneException('the call has not yet completed its task')

            if not hasattr(self, 'result'):
                self.result = self.queue.get(block=block, timeout=timeout)

            return self.result


def wait_tasks_results(asynchronous_tasks, delay=1, _log=log):

    while True:
        if isinstance(asynchronous_tasks, (list, tuple)):
            jobs_status = [_task.is_done() for _task in asynchronous_tasks]
        elif isinstance(asynchronous_tasks, dict):
            jobs_status = [asynchronous_tasks[_task].is_done() for _task in asynchronous_tasks.keys()]
        else:
            raise NotImplementedError

        if all(jobs_status):
            break
        else:
            _log.info("{} Tasks still running".format(len([s for s in jobs_status if not s])))
            time.sleep(delay)

    if isinstance(asynchronous_tasks, (list, tuple)):
        for _task in asynchronous_tasks:
            _task.join()
    elif isinstance(asynchronous_tasks, dict):
        for _task in asynchronous_tasks.keys():
            asynchronous_tasks[_task].join()
    else:
        raise NotImplementedError


def retry(exceptions, tries=4, delay=3, backoff=2, logger=log):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    MODIFIED: removed horrible CamelCase parameter


    :param exceptions: the exception to check. may be a tuple of
        exceptions to check
    :type exceptions: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance

    """

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = "{}, Retrying in {} seconds...".format(str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def check_result(check_func, custom_exception, logger=log):
    def deco_check(f):
        @wraps(f)
        def result_check(*args, **kwargs):
            res = f(*args, **kwargs)
            _args = collections.deque(args)
            _args.appendleft(res)
            if check_func(*_args, **kwargs):
                return res
            else:
                raise custom_exception("Check on result of {} failed".format(f.__name__))

        return result_check

    return deco_check


@contextmanager
def pool_context(psize, initializer=None, init_args=None):
    _pool = Pool(processes=psize,
                 initializer=initializer,
                 initargs=init_args if isinstance(init_args, tuple) else (init_args,))
    yield _pool
    _pool.close()
    _pool.join()


if __name__ == '__main__':
    # sample usage
    import random


    def check_f(*args, **kwargs):
        res = args[0]
        if res < 5:
            return False
        else:
            return True


    @check_result(check_func=check_f)
    def dummy():
        return 5


    @check_result(check_func=check_f)
    def other_dummy():
        return 4


    @retry(exceptions=(UnboundLocalError,), tries=2, delay=2, logger=log)
    def long_process(num):
        a = [True, False]
        time.sleep(5)
        ch = random.choice(a)
        if ch:
            raise UnboundLocalError("Fake exception in long process")
        return num * num


    @retry(exceptions=(UnboundLocalError,), tries=2, delay=2, logger=None)
    def long_long_process(num):
        a = [True, False]
        time.sleep(20)
        ch = random.choice(a)
        if ch:
            raise UnboundLocalError("Fake exception in long long process")
        return num * num


    @retry(exceptions=(UnboundLocalError,), tries=2, delay=2, logger=None)
    def long_long_long_process(num):
        a = [True, False]
        time.sleep(30)
        ch = random.choice(a)
        if ch:
            raise UnboundLocalError("Fake exception in long long long process")
        return num * num


    @asynchronous
    def long_worker(num):
        return long_process(num)


    @asynchronous
    def long_long_worker(num):
        return long_long_process(num)


    @asynchronous
    def long_long_long_worker(num):
        return long_long_long_process(num)


    result = long_worker.start(12)

    for i in range(20):
        print(i)
        time.sleep(1)

        if result.is_done():
            print("result {0}".format(result.get_result()))

    result2 = long_worker.start(13)

    try:
        print("result2 {0}".format(result2.get_result()))

    except asynchronous.NotYetDoneException as ex:
        print(ex.message)

    # @retry(exceptions=(UnboundLocalError,), tries=2, delay=2, logger=log)

    results = []
    for task in [long_worker, long_long_worker, long_long_long_worker]:
        results.append(task.start(15))

    wait_tasks_results(results)

    for res in results:
        print(res.get_result())