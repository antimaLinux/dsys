from dsys.src.logging.dsys_logger_client import get_logger

log = get_logger(__name__)

functions = dict()


class Strategy(object):
    def __init__(self, _function):
        self._function = _function

    def __call__(self, *args, **kwargs):
        log.info("Calling {} with params [{}] [{}]".format(self._function.__name__, args, kwargs))
        return self._function(*args, **kwargs)


def strategy_for(name, container):
    """
    Decorator used to define strategies
    @param name: strategy name
    @param container: strategy dictionary
    """

    def inner(func):
        def wrapped(*args, **kwargs):
            return func(*args, **kwargs)

        container[name] = Strategy(wrapped)
        return wrapped

    return inner


def print_func(*args, **_):
    return args[0]

