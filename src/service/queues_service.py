from dsys.src.config.configuration import queues_configuration
from dsys.src.utils.managers import QueueManager, SharedResultsManager, start_server
from dsys.src.utils.queues import IndexableQueue, PersistentQueue, JournaledPersistentQueue, TasksPriorityQueue
from Queue import Queue as SimpleQueue
from dsys.src.logging.dsys_logger_client import get_logger
import collections
import argparse

log = get_logger(__name__)

server_setup = collections.namedtuple('Setup', ['address', 'port', 'passkey'])

queues_setup = {
    'priority': TasksPriorityQueue,
    'simple': SimpleQueue,
    'indexable': IndexableQueue,
    'persistant': PersistentQueue,
    'journaled': JournaledPersistentQueue
}


def start_jobserver(queues_conf, queue_type='priority'):

    conf = server_setup(address=queues_conf['jobs_queue']['address'],
                        port=queues_conf['jobs_queue']['port'],
                        passkey=queues_conf['jobs_queue']['auth'])

    return start_server(QueueManager(address=conf.address, port=conf.port,
                                     authkey=conf.passkey, queues={'tasks_queue': queues_setup[queue_type]()}))


def start_results_server(queues_conf):
    conf = server_setup(address=queues_conf['results_queue']['address'],
                        port=queues_conf['results_queue']['port'],
                        passkey=queues_conf['results_queue']['auth'])

    return start_server(SharedResultsManager(address=conf.address, port=conf.port, authkey=conf.passkey))


def parse_commandline_args():
    parser = argparse.ArgumentParser('[dsys] Queues service')

    parser.add_argument(
        '--queue_type',
        default='priority',
        help='queue type',
        dest='queue_type',
    )
    parsed_args = parser.parse_args()
    return parsed_args


def main(args):
    servers = [
        start_jobserver(queues_configuration, args.queue_type),
        start_results_server(queues_configuration)
    ]

    for server in servers:
        log.info(server)

    for server in servers:
        server.join()


if __name__ == '__main__':
    main(parse_commandline_args())
