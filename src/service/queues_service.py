from src.config.configuration import queues_configuration
from src.utils.managers import QueueManager, SharedResultsManager, start_server
from src.utils.queues import queues_setup
from src.logging.dsys_logger_client import get_logger
import argparse

log = get_logger(__name__)


class ServerSetup(object):
    __slots__ = ('address', 'port', 'passkey')

    def __init__(self, address, port, passkey):
        self.address = address
        self.port = port
        self.passkey = passkey

    def __repr__(self):
        return "< ServerSetup address={} port={} passkey={}>".format(self.address, self.port, self.passkey)


def start_jobserver(queues_conf, queue_type='priority'):

    conf = ServerSetup(address=queues_conf['jobs_queue']['address'],
                       port=queues_conf['jobs_queue']['port'],
                       passkey=queues_conf['jobs_queue']['auth'])

    return start_server(QueueManager(address=conf.address, port=conf.port,
                                     authkey=conf.passkey, queues={'tasks_queue': queues_setup[queue_type]()}))


def start_results_server(queues_conf):
    conf = ServerSetup(address=queues_conf['results_queue']['address'],
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
