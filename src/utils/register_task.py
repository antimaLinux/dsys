from src.logging.dsys_logger_client import get_logger
from src.utils.tasks import task, delayed_task

log = get_logger(__name__)


@task(name='simple_print')
def print_function(*args, **kwargs):
    log.info("Executed f with args [{} {}]".format(args, kwargs))


@delayed_task(name='delay_print', wait=1.5)
def d_print_function(*args, **kwargs):
    log.info("Executed f with args [{} {}]".format(args, kwargs))


@delayed_task(name='fact')
def factorial_function(*args, **kwargs):
    import math
    res = math.factorial(args[0])
    log.info("Executed f with args [{} {}]: {}".format(args, kwargs, res))
    return res
