import logging
from logging import handlers

rootLogger = logging.getLogger('')
rootLogger.setLevel(logging.DEBUG)


def get_logger(name=__name__, loglevel=logging.INFO, host='localhost'):
    socket_handler = handlers.SocketHandler(host, handlers.DEFAULT_TCP_LOGGING_PORT)
    rootLogger.addHandler(socket_handler)
    logging.info("Creating new logger {}".format(name))
    new_logger = logging.getLogger(name)
    new_logger.setLevel(loglevel)
    return new_logger
