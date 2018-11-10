from Queue import Queue, PriorityQueue
import logging

log = logging.getLogger(__name__)

__author__ = 'Riccardo Vilardi'
__doc__ = """
Try to create a distributed queue accessable from many process from many ip
This is a test
TODO:
    -Create a class that is accessable through flask restful api
    -The class must be asynchronous and capable to handle many request
    -
"""


class IndexableQueue(Queue):
    def __getitem__(self, index):
        with self.mutex:
            return self.queue[index]


class TasksPriorityQueue(PriorityQueue):
    def __init__(self):
        PriorityQueue.__init__(self)
        self.counter = 0

    def put(self, item, priority):
        PriorityQueue.put(self, (priority, self.counter, item))
        self.counter += 1

    def get(self, *args, **kwargs):
        _, _, item = PriorityQueue.get(self, *args, **kwargs)
        return item


# class TasksPriorityQueue(PriorityQueue):
#     def __init__(self, *args, **kwargs):
#         super(TasksPriorityQueue, self).__init__(*args, **kwargs)
#         self.counter = 0
#
#     def put(self, item, priority=5):
#         super(TasksPriorityQueue, self).put((priority, self.counter, item))
#         self.counter += 1
#
#     def get(self, *args, **kwargs):
#         _, _, item = super(TasksPriorityQueue, self).get(*args, **kwargs)
#         return item


class SimpleQueue(Queue):
    pass


queues_setup = {
    'priority': TasksPriorityQueue,
    'simple': SimpleQueue,
    'indexable': IndexableQueue,
}
