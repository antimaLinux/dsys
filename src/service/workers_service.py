from dsys.src.config.configuration import queues_configuration
from dsys.src.utils.managers import ClientManager
import argparse
import time
import collections
from dsys.src.utils.workers import Worker
from dsys.src.logging.dsys_logger_client import get_logger

log = get_logger(__name__)

server_setup = collections.namedtuple('Setup', ['address', 'port', 'passkey'])


def init_job_client(queues_conf):
    conf = server_setup(address=queues_conf['jobs_queue']['address'],
                        port=queues_conf['jobs_queue']['port'],
                        passkey=queues_conf['jobs_queue']['auth'])

    return ClientManager(address=(conf.address, conf.port), authkey=conf.passkey, queues=['tasks_queue'])


def init_result_client(queues_conf):
    conf = server_setup(address=queues_conf['results_queue']['address'],
                        port=queues_conf['results_queue']['port'],
                        passkey=queues_conf['results_queue']['auth'])

    return ClientManager(address=(conf.address, conf.port), authkey=conf.passkey, queues=['results_queue'])


def get_queues(job_client, results_client):
    return job_client.get_tasks_queue(), results_client.get_results()


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func
    return decorate


@static_var('current_workers', None)
def workerpool(jobs, results, initial_workers=2, max_workers=8, poison_pill='-STOP-', consume_rate=10., sample_time=1):

    current_workers = collections.deque()

    def inner(_current_workers):
        for _ in range(initial_workers):
            worker = Worker(jobs, results, wait=False, poison_pill=poison_pill)
            yield worker
            _current_workers.append(worker)

        log.info("Started default workers")

        while True:
            _current_workers = collections.deque([w for w in _current_workers if w.is_alive()])
            delta = watchdog(jobs, consume_rate, _current_workers, initial_workers, max_workers, sample_time)
            if delta > 0:
                if len(_current_workers) < max_workers:
                    worker = Worker(jobs, results, wait=False, poison_pill=poison_pill)
                    yield worker
                    _current_workers.append(worker)
            elif delta < 0 and len(_current_workers) > initial_workers:
                stop_worker(jobs, poison_pill)
            else:
                pass

    return inner(current_workers)


def watchdog(jobs, consume_rate_setpoint, current_workers, initial_workers, max_workers, sample_time=1./15.):

    size_t0 = jobs.qsize()
    time.sleep(sample_time)
    size_t1 = jobs.qsize()

    rate = size_t0 - size_t1 / len(current_workers)

    if len(current_workers) < initial_workers:
        return 1

    if len(current_workers) >= max_workers:
        return -1

    return rate - consume_rate_setpoint


def stop_worker(jobs_queue, poison_pill, workers_to_kill=1):
    for _ in range(workers_to_kill):
        jobs_queue.put(poison_pill, 9999999)


def start_workers(queues_conf, initial_workers=2, max_workers=8, consume_rate=10., sample_time=1, poison_pill='-STOP-'):
    jobs, results = get_queues(init_job_client(queues_conf), init_result_client(queues_conf))
    pool = workerpool(jobs, results, initial_workers, max_workers, poison_pill,
                      consume_rate=consume_rate, sample_time=sample_time)
    for new_worker in pool:
        new_worker.deamon=True
        new_worker.start()


def parse_commandline_args():
    parser = argparse.ArgumentParser('[dsys] Queues service')

    parser.add_argument(
        '--initial_workers',
        default=3,
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
