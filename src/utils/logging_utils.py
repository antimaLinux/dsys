import os
from logging import getLogger
from dsys.src.config import loggingconfig as settings
from logging.handlers import RotatingFileHandler
import multiprocessing, threading, logging, sys, traceback

formatters = {
    'base': '%(asctime)s|%(levelname)s:%(name)s %(message)s',
    'medium': '%(asctime)s|%(process)d|%(module)s|%(levelname)s:%(name)s %(message)s',
    'high': '%(asctime)s|%(process)d|%(module)s|%(funcName)s|%(lineno)d|%(levelname)s:%(name)s[%(message)s]'
}


class Tag(dict):
    """
    Tag class to be used with log_message function
    TODO: add any useful field
    """
    def __init__(self, *args, **kwargs):
        self.customer_name = kwargs.pop('customer_name', 'DSYS').upper()
        self.script_name = kwargs.pop('script_name', '____').upper()
        super(Tag, self).__init__(*args, **kwargs)

    def __repr__(self):
        return "[{}][{}]".format(self.customer_name, self.script_name)


def log_message(tag, sep=' '):
    def add_tag_to_log_message(log_string):
        return str(tag) + sep + log_string
    return add_tag_to_log_message


class Logger(object):
    """
    %(name)s            Name of the logger (logging channel)
    %(levelno)s         Numeric logging level for the message (DEBUG, INFO,
                        WARNING, ERROR, CRITICAL)
    %(levelname)s       Text logging level for the message ("DEBUG", "INFO",
                        "WARNING", "ERROR", "CRITICAL")
    %(pathname)s        Full pathname of the source file where the logging
                        call was issued (if available)
    %(filename)s        Filename portion of pathname
    %(module)s          Module (name portion of filename)
    %(lineno)d          Source line number where the logging call was issued
                        (if available)
    %(funcName)s        Function name
    %(created)f         Time when the LogRecord was created (time.time()
                        return value)
    %(asctime)s         Textual time when the LogRecord was created
    %(msecs)d           Millisecond portion of the creation time
    %(relativeCreated)d Time in milliseconds when the LogRecord was created,
                        relative to the time the logging module was loaded
                        (typically at application startup time)
    %(thread)d          Thread ID (if available)
    %(threadName)s      Thread name (if available)
    %(process)d         Process ID (if available)
    %(message)s         The result of record.getMessage(), computed just as
                        the record is emitted
    """
    def __init__(self, name, tag=None):
        name = name.replace('.log', '')
        logger = getLogger('log_namespace.%s' % name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            file_name = os.path.join(settings.LOGGING_DIR, '%s.log' % name)
            handler = logging.FileHandler(file_name)
            formatter = logging.Formatter(formatters['high'])
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)
        self._logger = logger

    def get(self):
        return self._logger

    def add_tag(self, tag):
        pass


class MyCustomClass(object):

    def __init__(self):
        self.logger = Logger(self.__class__.__name__).get()

    def do_something(self):
        self.logger.info('Hello')

    def raise_error(self):
        self.logger.error('some error message')


class MultiProcessingLog(logging.Handler):
    def __init__(self, name, mode, maxsize, rotate):
        logging.Handler.__init__(self)

        self._handler = RotatingFileHandler(name, mode, maxsize, rotate)
        self.queue = multiprocessing.Queue(-1)

        t = threading.Thread(target=self.receive)
        t.daemon = True
        t.start()

    def setFormatter(self, fmt):
        logging.Handler.setFormatter(self, fmt)
        self._handler.setFormatter(fmt)

    def receive(self):
        while True:
            try:
                record = self.queue.get()
                self._handler.emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)

    def send(self, s):
        self.queue.put_nowait(s)

    def _format_record(self, record):
        # ensure that exc_info and args
        # have been stringified.  Removes any chance of
        # unpickleable things inside and possibly reduces
        # message size sent over the pipe
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            dummy = self.format(record)
            record.exc_info = None

        return record

    def emit(self, record):
        try:
            s = self._format_record(record)
            self.send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        self._handler.close()
        logging.Handler.close(self)


if __name__ == '__main__':
    instance = MyCustomClass()
    instance.do_something()
    instance.raise_error()
