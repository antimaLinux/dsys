try:
    import simplejson as json
except ImportError:
    import json


class MyJsonEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        super(self.__class__, self).__init__(**kwargs)

    def default(self, o):
        """
        EXAMPLE OF DEFAULT
        def default(self, o):
            try:
                iterable = iter(o)
            except TypeError:
                pass
            else:
                return list(iterable)
            # Let the base class default method raise the TypeError
            return JSONEncoder.default(self, o)
        :param o: object to serialize
        :return: serialized object
        """
        # TODO HERE GOES CASES
        if o:
            return o.__dict__
        return json.JSONEncoder.default(self, o)


class MyjsonDecoder(json.JSONDecoder):
    def __init__(self, **kwargs):
        super(self.__class__, self).__init__(**kwargs)


class JsonHandler(object):
    def __init__(self, **kwargs):
        self.encoder = MyJsonEncoder()
        self.decoder = MyjsonDecoder()

    def dump(self, obj):
        dumped = None
        try:
            dumped = json.dump(obj)
        except Exception as e:
            print("Unable to dump object")
        finally:
            return dumped

    def dumps(self, obj, cmplx=False):
        dumped = None
        try:
            dumped = json.dumps(obj, cls=self.encoder if cmplx else None)
        except Exception as e:
            print("Unable to dump object")
        finally:
            return dumped

    def load(self, obj):
        loaded = None
        try:
            loaded = json.load(obj)
        except Exception as e:
            print("Unable to load object")
        finally:
            return loaded

    def loads(self, obj):
        loaded = None
        try:
            loaded = json.loads(obj)
        except Exception as e:
            print("Unable to load object")
        finally:
            return loaded

    def decode(self, obj, object_hook=None):
        assert object_hook, "Unable to use hook decode function"
        decoded = None
        setattr(self.decoder, 'object_hook', object_hook)
        try:
            decoded = self.decoder.decode(obj)
        except Exception as e:
            print("Unable to decode object")
        finally:
            return decoded

    def encode(self, obj):
        encoded = None
        try:
            encoded = self.encoder.encode(obj)
        except Exception as e:
            print("Unable to encode object")
        finally:
            return encoded

if __name__ == '__main__':
    class MyNewSerializableClass(object):
        def __init__(self):
            self.var1 = None
            self.var2 = 'String'
            self.var3 = 4

        def dummy_function(self):
            return self.__repr__()

    class_ = MyNewSerializableClass()

    handler = JsonHandler()
    serialized = handler.dumps({'a':1, 'b':2})
    print serialized
    print handler.loads(serialized)
    serialized = handler.encode(class_)
    print serialized
    print handler.dumps(class_, cmplx=True)


