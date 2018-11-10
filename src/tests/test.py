import uuid
import time
from src.utils.register_task import print_function, d_print_function, factorial_function

from src.config.configuration import queues_configuration
from src.service.workers_service import get_queues, init_job_client, init_result_client
from src.utils.tasks import Task


def main():
    time.sleep(3)
    enqueued = set()
    counter = 3
    jobs, results = get_queues(init_job_client(queues_configuration), init_result_client(queues_configuration))
    while counter > 0:
        for n in range(10):
            _id = str(uuid.uuid4())
            jobs.put(Task(_id, print_function, n), 200)
            enqueued.add(_id)

        for n in range(10000):
            _id = str(uuid.uuid4())
            jobs.put(Task(_id, print_function, n), 500)
            enqueued.add(_id)

        for n in range(10):
            _id = str(uuid.uuid4())
            jobs.put(Task(_id, factorial_function, n), 200)
            enqueued.add(_id)

        time.sleep(3)

        for k in enqueued:
            try:
                print(k, results[k])
            except KeyError:
                print("Job not ready")

        counter -= 1


if __name__ == '__main__':
    main()
