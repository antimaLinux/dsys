from dsys.src.utils.tasks import Task
from dsys.src.config.configuration import queues_configuration
from dsys.src.service.workers_service import get_queues, init_job_client, init_result_client
import uuid
from dsys.src.utils.registered_tasks import print_func
import time


def main():
    jobs, results = get_queues(init_job_client(queues_configuration), init_result_client(queues_configuration))

    while True:
        for n in range(10):
            jobs.put(Task(uuid.uuid4(), print_func, n).to_dict, 200)

        if results and len(results) > 0:
            for k in results.keys():
                print(k, results[k])
        time.sleep(5)

if __name__ == '__main__':
    main()
