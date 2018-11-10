from __future__ import print_function, unicode_literals
import multiprocessing
from multiprocessing.connection import Listener, Client
from Queue import Empty, Full
from threading import Thread, active_count

from src.utils.queues import PersistentQueue


class ShutDown(Exception):
    pass


class RPCServer(multiprocessing.Process):
    def __init__(self, address, authkey):
        multiprocessing.Process.__init__(self)
        self._functions = {}
        self.active_threads_num = 0
        self.stay_alive = True
        self._server_c = Listener(address, authkey=authkey)

    def register_queues(self, queues):
        """
        DO NOT USE AT THE MOMENT. This must be implemented obtaining queues from ClientManagers
        @param queues: 
        @return: 
        """
        for q_name, queue in queues.items():
                setattr(self, q_name, queue)

    def register_class_function(self, func):
        self.add_method(func)
        self._functions[func.__name__] = getattr(self, func.__name__)

    def add_method(self, method, name=None):

        if name is None:
            name = method.func_name

        class new(self.__class__):
            pass

        setattr(new, name, method)
        self.__class__ = new

    def register_function(self, func):
        self._functions[func.__name__] = func

    def serve_forever(self):

        while True:
            client_c = self._server_c.accept()
            t = Thread(target=self.handle_client, args=(client_c,))
            t.daemon = True
            t.start()

            self.active_threads_num = active_count()

    def handle_client(self, client_c):
        while True:

            func_name, args, kwargs = client_c.recv()
            try:
                r = self._functions[func_name](*args,**kwargs)
                client_c.send(r)
            except Exception as e:
                client_c.send(e)

    def run(self):
        self.serve_forever()

    def __repr__(self):
        return "< RPC server > -- serving at {}".format(self._server_c.address)


class RPCProxy(object):
    def __init__(self, address, authkey):
        self._conn = Client(address, authkey=authkey)

    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            self._conn.send((name, args, kwargs))
            result = self._conn.recv()
            if isinstance(result, Exception):
                raise result
            return result
        return do_rpc

    def __repr__(self):
        return "< RPC client > -- connected"


def add(x, y):
    return x+y


def sub(x, y):
    return x-y


def get_job(cls, q_name):
    try:
        return getattr(cls, q_name).get_nowait()
    except Empty:
        print("Queue Empty!!")
        return None


def put_job(cls, value, q_name):
    """
    Consumers must be up! ;-)
    @param cls: 
    @param value: 
    @param q_name: 
    @return: 
    """
    try:
        getattr(cls, q_name).put_nowait(value)
    except Full:
        print("Queue Full!!")
        pass


def get_active_clients(cls):
    return cls.active_threads


def shutdown(cls):
    raise ShutDown("Server shutting down!")


def task_done(cls, q_name):
    getattr(cls, q_name).task_done()


def task_join(cls, q_name):
    getattr(cls, q_name).join()


class Task(object):
    def __init__(self, func, args):
        self.func = func
        self.args = args

    def __call__(self):

        if isinstance(self.args, tuple) or isinstance(self.args, list):
            return self.func(*self.args)
        elif isinstance(self.args, dict):
            return self.func(**self.args)
        else:
            return self.func(self.args)

    def __repr__(self):
        return "Enqueued %s(%r)" % (self.__class__, self.__dict__)


register_functions = [get_job, put_job, get_active_clients, shutdown, task_done]


def get_server(address='localhost', port=9999, authkey='mykey', queues={}):

    server = RPCServer((address, int(port)), authkey=str(authkey))

    for func in register_functions:
        server.register_class_function(func)

    server.register_queues(queues)

    return server


def get_client(address='localhost', port=9999, authkey='mykey'):
    return RPCProxy((address, int(port)), authkey=str(authkey))



def test():
    def dummy_function(*args, **kwargs):
        import math
        # args[1].put('Processed {}:'.format(args[0]))
        return math.factorial(args[0])

    rpc_server = get_server(queues={'jobs': PersistentQueue(str("/tmp/queue_storage_dir"))})

    rpc_server.register_function(add)
    rpc_server.register_function(sub)
    rpc_server.start()
    print(rpc_server)

    client = get_client()
    print(client.get_active_clients())
    for n in range(100):
        client.put_job(n, 'jobs')

    client.put_job({str(n): n for n in range(10)}, 'jobs')

    client.put_job(Task(dummy_function, 10), 'jobs')

    client2 = RPCProxy(('localhost', 9999), authkey=str('mykey'))
    print(client2.get_active_clients())

    for n in range(101):
        print(client2.get_job('jobs'))

    print(client2.get_job('jobs')())

    print(client2.get_active_clients())

    print(client.add(20, 50))
    print(client2.get_active_clients())

if __name__ == '__main__':
    pass
