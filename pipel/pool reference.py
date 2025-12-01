import time
import random
from multiprocessing import Process, Queue
import multiprocessing


class Worker(Process):
    def __init__(self, worker_id, task_queue, result_queue, status_queue):
        super().__init__()
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.status_queue = status_queue

    def run(self):
        """Worker loop: wait → work → return result → mark available."""
        while True:
            # Ask pool for job
            self.status_queue.put((self.worker_id, "available"))

            # Wait for a job
            job = self.task_queue.get()

            if job == "STOP":
                break

            # Simulate work
            wait_time = random.randint(1, 5)
            time.sleep(wait_time)
            result = random.randint(100, 999)

            # Send result back
            self.result_queue.put((self.worker_id, result))


class WorkerPool:
    def __init__(self, max_workers):
        self.max_workers = max_workers

        self.task_queues = []
        self.results = Queue()
        self.status = Queue()

        # Spawn workers
        self.workers = []
        for i in range(max_workers):
            tq = Queue()
            self.task_queues.append(tq)

            w = Worker(i, tq, self.results, self.status)
            w.start()
            self.workers.append(w)

        # Availability table
        self.available = {i: False for i in range(max_workers)}

    def wait_for_available(self):
        """Wait for at least one worker to report 'available'."""
        worker_id, stat = self.status.get()
        if stat == "available":
            self.available[worker_id] = True

    def submit(self, job):
        """Submit a job to the first available worker."""
        # Wait until someone is free
        free_worker = None
        while free_worker is None:
            # Refresh worker availability
            self.wait_for_available()

            for wid, is_free in self.available.items():
                if is_free:
                    free_worker = wid
                    break

        # Mark worker as busy
        self.available[free_worker] = False

        # Send job to that worker’s queue
        self.task_queues[free_worker].put(job)

    def get_result(self):
        """Get the next completed job result."""
        worker_id, value = self.results.get()
        return worker_id, value

    def stop(self):
        """Stop all worker processes gracefully."""
        for tq in self.task_queues:
            tq.put("STOP")

        for w in self.workers:
            w.join()


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")  # safe cross-platform

    pool = WorkerPool(max_workers=2)

    print("Submitting 10 jobs:")
    for i in range(10):
        pool.submit(f"JOB-{i}")
        print(f" submitted JOB-{i}")

    print("\nCollecting results:")
    for i in range(10):
        wid, result = pool.get_result()
        print(f" Worker {wid} returned: {result}")

    pool.stop()
    print("\nAll workers stopped.")
