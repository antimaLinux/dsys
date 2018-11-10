from Queue import Empty
from multiprocessing import Process
import sys
import os
from src.logging.dsys_logger_client import get_logger
from src.utils.tasks import Task
import simplejson as json
import functools
import signal
import psutil

log = get_logger(__name__)

SHARED_DICT = 1
STD_QUEUE = 0


class Worker(Process):
    class ShutdownSignaException(Exception):
        pass

    def __init__(self, task_queue, result_queue, wait=True, poison_pill=None, registered_functions=None):
        super(Worker, self).__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.results_queue_type = self.check_queue_type(result_queue)
        self.wait_data = wait
        self.poison_pill = poison_pill
        self.registered_functions = registered_functions or {}

    @property
    def pid(self):
        return os.getpid()

    def shudown(self, signum, func=None):
        if signum in (signal.SIGTERM, signal.SIGUSR1, signal.SIGINT):
            raise self.ShutdownSignaException("Called Worker Shutdown [Signal {} -- Func {}], exit".format(signum,
                                                                                                           func))

    @property
    def stats(self):
        proc = psutil.Process(self.pid)
        return proc.as_dict(attrs=['cpu_percent', 'memory_percent', 'name', 'status', 'username', 'pid'])

    @staticmethod
    def check_queue_type(queue):
        if all([hasattr(queue, method) for method in ['keys', 'get', 'update']]):
            return SHARED_DICT
        else:
            return STD_QUEUE

    def _decode(self, job):

        def _hook(data, _registered):
            if isinstance(data, list):
                return [Task.from_dict(d, _registered) for d in data]
            else:
                return [Task.from_dict(data, _registered)]

        if isinstance(job, list) and all([isinstance(j, Task) for j in job]):
            return job
        elif isinstance(job, Task):
            return [job]
        else:
            return json.loads(job, object_hook=functools.partial(_hook, _registered=self.registered_functions))

    def _encode(self, result):
        return json.dumps(result)

    def _enqueue(self, outdict):
        if self.results_queue_type == SHARED_DICT:
            for k in outdict.keys():
                self.result_queue[k] = self._encode(outdict[k])
        else:
            self.result_queue.put(self._encode(outdict))

    @staticmethod
    def is_iterable(data):
        try:
            _ = iter(data)
            return True if not isinstance(data, dict) else False
        except TypeError:
            log.info('{} is not iterable'.format(data))
            return False

    def run(self):

        signal.signal(signal.SIGTERM, self.shudown)
        signal.signal(signal.SIGUSR1, self.shudown)
        signal.signal(signal.SIGINT, self.shudown)

        proc_name = 'Consumer - ' + self.name

        for job in iter(self.task_queue.get, self.poison_pill):
            try:
                tasks = self._decode(job)
                outdict = {t.id: t() for t in tasks}
                self._enqueue(outdict)
                self.task_queue.task_done()
            except Empty:
                pass
            except StopIteration:
                break
            except self.ShutdownSignaException:
                break
            except Exception as e:
                log.error('{}: Encountered an error -- {}'.format(proc_name, str(e)))

        sys.exit(0)
