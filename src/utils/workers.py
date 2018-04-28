from Queue import Empty
from multiprocessing import Process
import sys
from dsys.src.logging.dsys_logger_client import get_logger
from dsys.src.utils.tasks import Task

log = get_logger(__name__)

DICT_QUEUE = 1
STD_QUEUE = 0


class Worker(Process):
    def __init__(self, task_queue, result_queue, wait=True, poison_pill=None):
        super(Worker, self).__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.queue_type = self.check_queue_type(result_queue)
        self.wait_data = wait
        self.poison_pill = poison_pill

    @staticmethod
    def check_queue_type(queue):
        if all([hasattr(queue, method) for method in ['keys', 'get', 'update']]):
            return DICT_QUEUE
        else:
            return STD_QUEUE

    @staticmethod
    def is_iterable(data):
        try:
            _ = iter(data)
            return True if not isinstance(data, dict) else False
        except TypeError:
            log.info('{} is not iterable'.format(data))
            return False

    def run(self):

        proc_name = 'Consumer - ' + self.name

        for job in iter(self.task_queue.get, self.poison_pill):
            try:
                if self.is_iterable(job):

                    tasks = []

                    for j in job:
                        tasks.append((j['id'], Task.from_dict(j)))

                    outdict = {t[0]: t[1]() for t in tasks}

                    log.info('{}'.format(outdict))

                else:
                    task = Task.from_dict(job)
                    outdict = {task.id: task()}

                    log.info('{}'.format(outdict))

                if self.queue_type == DICT_QUEUE:
                    for k in outdict.keys():
                        self.result_queue[k] = outdict[k]
                else:
                    self.result_queue.put(outdict)
            except Empty:
                pass
            except StopIteration:
                break
            except Exception as e:
                log.error('{}: Encountered an error -- {}'.format(proc_name, e))

        sys.exit(0)
