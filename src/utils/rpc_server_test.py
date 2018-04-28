from __future__ import print_function, unicode_literals

# Server service
try:
    import SimpleXMLRPCServer
except ImportError:
    import xmlrpc.server as SimpleXMLRPCServer

import collections
import simplejson as json


class StringFunctions:
    def __init__(self):

        self.python_strings = set()
        self.dict_strings = dict()
        self.counter = 0

    def _privateFunction(self):
        # This function cannot be called through XML-RPC because it
        # starts with an '_'
        pass

    def chop_in_half(self, astr):
        if astr not in self.python_strings:
            self.python_strings.add(astr)
            self.dict_strings[str(self.counter)] = astr
            self.counter += 1

        return astr[:len(astr) / 2]

    def repeat(self, astr, times):
        if astr not in self.python_strings:
            self.python_strings.add(astr)
            self.dict_strings[str(self.counter)] = astr
            self.counter += 1

        return astr * times

    def all_strings(self):
        return list(self.python_strings)

    def all_strings_json(self):
        return json.dumps(self.dict_strings)


class TransformedDict(collections.MutableMapping):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, transform_func=lambda x: x, *args, **kwargs):
        self.store = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys
        self.transform_function = transform_func

    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return self.transform_function(key)


class RemoteJob(TransformedDict):
    def __init__(self, id, status, user):
        super(RemoteJob, self).__init__(transform_func=lambda x: x.lower(), id=id, status=status, user=user)

        # TODO params

    @property
    def json(self):
        return json.dumps(self.store)

    def __repr__(self):
        return "< RemoteJob : {}>".format(self.store)


class RemoteJobResponse(TransformedDict):
    def __init__(self, id, status, user, result):
        super(RemoteJobResponse, self).__init__(transform_func=lambda x: x.lower(), id=id,
                                                status=status, user=user, result=result)

    @property
    def json(self):
        return json.dumps(self.store)

    def __repr__(self):
        return "< RemoteJobResponse : {}>".format(self.store)


class RPCHandler(object):
    def __init__(self, max_jobs=1, functions={}):
        self._functions = {}
        self._active_jobs = {}
        self._max_job = max_jobs
        self._register(functions)

    @property
    def _is_busy(self):
        return bool(self._active_jobs) and len(self._active_jobs.keys()) >= self._max_job

    def get_job(self, _id):
        if _id in self._active_jobs:
            pass
        else:
            resp = RemoteJobResponse(_id, "unknown_job", "", "")
            return resp.json

    def put_job(self, job):
        pass

    def _job_status(self, _id):
        pass

    def _register(self, functions):
        for func_name, func in functions:
            self._functions[func_name] = func

    def _run(self):
        pass  # TODO run parallel

    # TODO handle single job in a separated process (is alive monitoring)


server = SimpleXMLRPCServer.SimpleXMLRPCServer(("localhost", 8000))
server.register_instance(StringFunctions())
server.register_function(lambda astr: '_' + astr, '_string')
server.serve_forever()
