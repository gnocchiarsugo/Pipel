from typing import List
from multiprocessing import Queue
from .pool_component import PipelPool

class ManagedPipeline:
    """Sequential pipeline of PipelPools"""
    pipe_pools: List[PipelPool]
    queues: List[Queue] # queues[0] -> first module -> queues[1] -> second module -> queues[2] -> ...
    __out_queue_external_init: bool
    
    def __init__(self, pipe_pools:List[PipelPool], in_queue:Queue=None, out_queue:Queue=None):
        self.__in_queue_external_init = True if in_queue else False
        in_queue = in_queue or Queue()
        self.__out_queue_external_init = True if out_queue else False
        out_queue = out_queue or Queue()
        self.pipe_pools = pipe_pools
        self.queues = []
        self.__init_queues(in_queue=in_queue, out_queue=out_queue)
    
    def put(self, *args, **kwargs):
        self.queues[0].put((args, kwargs))

    def get(self):
        return self.pipe_pools[-1].get()
        
    def __init_queues(self, in_queue:Queue=None, out_queue:Queue=None):
        self.queues.append(in_queue)
        for i in range(len(self.pipe_pools) - 1):
            self.queues.append(Queue())
            self.pipe_pools[i].refresh(in_queue=self.queues[-2], out_queue=self.queues[-1])
        self.queues.append(out_queue)
        self.pipe_pools[-1].refresh(in_queue=self.queues[-2], out_queue=self.queues[-1])
        
    def refresh_pipes(self, in_queue:Queue=None, out_queue:Queue=None):
        # Close old queues
        for q in self.queues[1:-1]:
            q.close()
            q.join_thread()
        
        # close if internally managed
        if not self.__in_queue_external_init:
            self.queues[0].close()
            self.queues[0].join_thread()
        if not self.__out_queue_external_init:
            self.queues[-1].close()
            self.queues[-1].join_thread()
        self.queues.clear()
        
        self.__in_queue_external_init = True if in_queue else False
        in_queue = in_queue or Queue()
        self.__out_queue_external_init = True if out_queue else False
        out_queue = out_queue or Queue()
        
        self.queues.append(in_queue)
        for i in range(len(self.pipe_pools) - 1):
            self.queues.append(Queue())
            self.pipe_pools[i].refresh(in_queue=self.queues[-2], out_queue=self.queues[-1])
        self.queues.append(out_queue)
        self.pipe_pools[-1].refresh(in_queue=self.queues[-2], out_queue=self.queues[-1])
    
    def close(self):
        for pipe in self.pipe_pools:
            pipe.close()
        for q in self.queues[1:-1]:
            q.close()
            q.join_thread()
            
        if not self.__in_queue_external_init:
            self.queues[0].close()
            self.queues[0].join_thread()
        if not self.__out_queue_external_init:
            self.queues[-1].close()
            self.queues[-1].join_thread()
    
    # Add worker API
    def add_worker(self, component_index: int, amount:int = 1):
        self.pipe_pools[component_index].add_workers(amount)
    
    # Remove worker API
    def remove_worker(self, component_index: int, amount:int = 1):
        self.pipe_pools[component_index].remove_workers(amount)
    
    
    
    
    
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __len__(self):
        return len(self.pipe_pools)


__all__ = [
    'ManagedPipeline'
]