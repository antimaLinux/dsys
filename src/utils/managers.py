import socket
import time
from multiprocessing import Manager, Process
from multiprocessing.managers import BaseManager, DictProxy
from random import seed, randint
from src.logging.dsys_logger_client import get_logger

log = get_logger(__name__)


class QueueManager(BaseManager):
    """
    Job/queue server Manager
    """

    def __init__(self, *args, **kwargs):
        self.queues_registered = []

        for q_name, queue in kwargs.pop('queues', {}).items():
            QueueManager.register('get_' + q_name, callable=lambda: queue)
            self.queues_registered.append(q_name)

        # Register class methods
        QueueManager.register('get_server_info', callable=self.get_server_info)
        QueueManager.register('close_server', callable=self.shutdown_server)

        super(QueueManager, self).__init__(address=(kwargs.pop('address', 'localhost'),
                                                    kwargs.pop('port', 9999)),
                                           authkey=kwargs.pop('authkey', 'mykey'))

        # self.process()

    def get_server_info(self):
        """
        Use json. Info to send to client
        @return: 
        """
        return str({
            'address': str(self.address),
            'queues': str(self.queues_registered),
            'authkey': str(self._authkey)
        })

    def shutdown_server(self):
        self.shutdown()

    def process(self):
        self.start()

    def __repr__(self):
        return "< Queue manager server > -- serving at {} -- Queues registered: {}".format(self.address,
                                                                                           self.queues_registered)


class ClientManager(BaseManager):
    """
    Client manager
    """

    def __init__(self, *args, **kwargs):

        if len(kwargs) == 0 and len(args) != 0:
            kwargs.update(args[0])

        self.queues_registered = []

        for q_name in kwargs.pop('queues', []):
            ClientManager.register('get_' + q_name)
            self.queues_registered.append(q_name)

        # register server class method
        ClientManager.register('get_server_info')
        ClientManager.register('close_server')
        ClientManager.register('get_results')

        super(ClientManager, self).__init__(address=kwargs.pop('address', None), authkey=kwargs.pop('authkey', None))

        self.process()

    def process(self):
        self.connect()
        log.info("Connected to: {0!s}".format(self.address))

    def shutdown_client(self):
        self.shutdown()

    def __repr__(self):
        return "< Queue manager client > -- connected to {}".format(self.address)


class SharedResultsManager(BaseManager):
    """
    Job/queue server Manager
    """

    def __init__(self, *args, **kwargs):
        self.queues_registered = []
        queue = Manager().dict()

        SharedResultsManager.register('get_results', callable=lambda: queue, proxytype=DictProxy)

        # Register class methods
        SharedResultsManager.register('close_server', callable=self.shutdown_server)

        super(SharedResultsManager, self).__init__(address=(kwargs.pop('address', 'localhost'),
                                                            kwargs.pop('port', 9999)),
                                                   authkey=kwargs.pop('authkey', 'mykey'))

        # self.process()

    def shutdown_server(self):
        self.shutdown()

    def process(self):
        self.start()

    def __repr__(self):
        return "< Queue manager server > -- serving at {} -- Queues registered: {}".format(self.address,
                                                                                           self.queues_registered)


def start_server(instance):
    def serve(_instance):
        server = _instance.get_server()
        server.serve_forever()

    s = Process(target=serve, args=(instance,))
    s.start()

    return s


class HostUtils(object):

    @staticmethod
    def test_port(port):
        """
        https://docs.python.org/2/library/socket.html#example
        """
        while True:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.bind(('', port))  # Try to open port
            except OSError as e:
                if e.errno is 98:  # 98 means address already bound
                    return True
                raise e
            s.close()
            return False

    @classmethod
    def get_port(cls):
        seed(time.time())
        port = None
        while port is None:
            attempt = randint(5001, 9999)
            if not cls.test_port(attempt):
                port = attempt
            else:
                pass

        return port

    @staticmethod
    def get_host():
        return socket.gethostbyname(socket.gethostname())

    @staticmethod
    def get_hostname():
        return socket.gethostname()
