from src.config.configuration import queues_configuration
from src.utils.managers import ClientManager
import argparse
import collections
from src.utils.workers import Worker
from src.logging.dsys_logger_client import get_logger
from multiprocessing import Process
import time
import os
import signal

log = get_logger(__name__)


class ServerSetup(object):

    __slots__ = ('address', 'port', 'passkey')

    def __init__(self, address, port, passkey):
        self.address = address
        self.port = port
        self.passkey = passkey

    def __repr__(self):
        return "< ServerSetup address={} port={} passkey={}>".format(self.address, self.port, self.passkey)


class Watchdog(Process):
    def __init__(self, current_workers, jobs_queue, results_queue, rate, initial_workers, max_workers, sample_time,
                 poison_pill, logger):
        super(Watchdog, self).__init__()
        self.current_workers = current_workers
        self.jobs_queue = jobs_queue
        self.results_queue = results_queue
        self.rate = rate
        self.initial_workers = initial_workers
        self.max_workers = max_workers
        self.sample_time = sample_time
        self.poison_pill = poison_pill
        self.logger = logger

    def serve_forever(self):
        self.run()

    def run(self):
        while True:
            self.current_workers = collections.deque([w for w in self.current_workers if w.is_alive()])
            delta = analyze(self.jobs_queue, self.rate, self.current_workers,
                            self.initial_workers, self.max_workers, self.sample_time)
            if delta > 0 and len(self.current_workers) < self.max_workers:

                self.logger.info("====> Creating new worker: delta={}, threshold={}".format(delta, self.rate))
                worker = Worker(self.jobs_queue, self.results_queue, wait=False, poison_pill=self.poison_pill)
                worker.deamon = True
                worker.start()
                self.current_workers.append(worker)
                self.logger.info('{} - delta={}'.format(str(self), delta))

            elif delta < 0 and len(self.current_workers) > self.initial_workers:

                self.logger.info("====> Killing worker: delta={}, threshold={}".format(delta, self.rate))

                stop_worker(jobs_queue=self.jobs_queue,
                            poison_pill=self.poison_pill,
                            current_workers=self.current_workers,
                            workers_to_kill=1)

                self.current_workers = collections.deque([w for w in self.current_workers if w.is_alive()])
                self.logger.info('{} - delta={}'.format(str(self), delta))

            elif delta == self.rate and len(self.current_workers) > self.initial_workers:

                self.logger.info("====> Killing worker: delta={}, threshold={}".format(delta, self.rate))

                stop_worker(jobs_queue=self.jobs_queue,
                            poison_pill=self.poison_pill,
                            current_workers=self.current_workers,
                            workers_to_kill=len(self.current_workers) - self.initial_workers)

                self.current_workers = collections.deque([w for w in self.current_workers if w.is_alive()])
                self.logger.info('{} - delta={}'.format(str(self), delta))

            else:
                self.logger.info('{} - delta={}'.format(str(self), delta))

            time.sleep(1)

    def __repr__(self):
        return "< WatchDog - Running: {} Max: {}>".format(len(self.current_workers), self.max_workers)


def init_job_client(queues_conf):
    conf = ServerSetup(address=queues_conf['jobs_queue']['address'],
                       port=queues_conf['jobs_queue']['port'],
                       passkey=queues_conf['jobs_queue']['auth'])

    return ClientManager(address=(conf.address, conf.port), authkey=conf.passkey, queues=['tasks_queue'])


def init_result_client(queues_conf):
    conf = ServerSetup(address=queues_conf['results_queue']['address'],
                       port=queues_conf['results_queue']['port'],
                       passkey=queues_conf['results_queue']['auth'])

    return ClientManager(address=(conf.address, conf.port), authkey=conf.passkey, queues=['results_queue'])


def get_queues(job_client, results_client):
    return job_client.get_tasks_queue(), results_client.get_results()


def workerpool(jobs, results, initial_workers=2, max_workers=8, poison_pill='-STOP-', consume_rate=10., sample_time=1):

    current_workers = collections.deque()

    def inner(_current_workers):
        for _ in range(initial_workers):
            worker = Worker(jobs, results, wait=False, poison_pill=poison_pill)
            worker.deamon = True
            worker.start()
            _current_workers.append(worker)

        log.info("Started default workers")

        return Watchdog(current_workers=_current_workers,
                        jobs_queue=jobs,
                        results_queue=results,
                        rate=consume_rate,
                        initial_workers=initial_workers,
                        max_workers=max_workers,
                        sample_time=sample_time,
                        poison_pill=poison_pill,
                        logger=get_logger())

    return inner(current_workers)


def analyze(jobs, consume_rate_setpoint, current_workers, initial_workers, max_workers, sample_time=1. / 15.):

    size_t0 = jobs.qsize()
    time.sleep(sample_time)
    size_t1 = jobs.qsize()

    rate = size_t0 - size_t1 / len(current_workers)

    if len(current_workers) < initial_workers:
        return 1

    if len(current_workers) > max_workers:
        return -1

    return rate - consume_rate_setpoint


def stop_worker(jobs_queue, poison_pill, current_workers, workers_to_kill=1):

    if poison_pill is not None:

        _ = [jobs_queue.put(poison_pill, 1) for _ in range(workers_to_kill)]

    else:

        workers = [w.stats for w in current_workers]
        workers = [w for w in workers if w['cpu_percent'] == 0.]
        workers = sorted(workers, key=lambda x: x['memory_percent'])
        _ = [os.kill(workers[i * -1]['pid'], signal.SIGTERM) for i in range(workers_to_kill)]


def start_workers(queues_conf, initial_workers=2, max_workers=8, consume_rate=10., sample_time=1, poison_pill='-STOP-'):
    jobs, results = get_queues(init_job_client(queues_conf), init_result_client(queues_conf))
    pool = workerpool(jobs, results, initial_workers, max_workers, poison_pill,
                      consume_rate=consume_rate, sample_time=sample_time)
    pool.serve_forever()


def parse_commandline_args():
    parser = argparse.ArgumentParser('[dsys] Queues service')

    parser.add_argument(
        '--initial_workers',
        default=1,
        type=int,
        help='min workers',
        dest='initial_workers',
    )
    parser.add_argument(
        '--max_workers',
        default=6,
        type=int,
        help='max workers',
        dest='max_workers',
    )
    parser.add_argument(
        '--rate',
        default=10.,
        type=float,
        help='consuming rate threshold',
        dest='consume_rate',
    )
    parser.add_argument(
        '--sample_time',
        default=1.,
        type=float,
        help='check sample time',
        dest='sample_time',
    )
    parser.add_argument(
        '--poison_pill',
        default='-STOP-',
        type=str,
        help='message for stop',
        dest='poison_pill',
    )
    parsed_args = parser.parse_args()
    return parsed_args


def main():
    args = parse_commandline_args()
    start_workers(queues_configuration, args.initial_workers, args.max_workers,
                  args.consume_rate, args.sample_time, args.poison_pill)


if __name__ == '__main__':
    main()
