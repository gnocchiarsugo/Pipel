from typing import List
from multiprocessing import Queue
from .pool_component import PipelPool

class ManagedPipeline():
    """Sequential pipeline of PipelPools"""
    pipe_pools: List[PipelPool]
    queues: List[Queue] # queues[0] -> first module -> queues[1] -> second module -> queues[2] -> ...
        
    def __init__(self, pipe_pools:List[PipelPool], in_queue=Queue(), out_queue=Queue()):
        self.pipe_pools = pipe_pools
        self.queues = []
        self.refresh_pipes(in_queue=in_queue, out_queue=out_queue)
    
    def put(self, *args, **kwargs):
        self.queues[0].put((args, kwargs))

    def get(self):
        return self.pipe_pools[-1].get()
        
    def refresh_pipes(self, in_queue=Queue(), out_queue=Queue()):
        self.queues.clear()
        self.queues.append(in_queue)
        for i in range(len(self.pipe_pools) - 1):
            self.pipe_pools[i].in_queue = self.queues[-1]
            self.queues.append(Queue())
            self.pipe_pools[i].out_queue = self.queues[-1]
            self.pipe_pools[i].refresh(num_workers=1)
        self.pipe_pools[-1].in_queue = self.queues[-1]
        self.queues.append(out_queue)
        self.pipe_pools[-1].out_queue = self.queues[-1]
        self.pipe_pools[-1].refresh(num_workers=1)
        return in_queue, out_queue
    
    def close(self):
        for pipe in self.pipe_pools:
            pipe.close()
        for q in self.queues:
            q.close()
            q.join_thread()
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __len__(self):
        return len(self.pipe_pools)


__all__ = [
    'ManagedPipeline'
]