from typing import List
from multiprocessing import Queue
from .pool_component import PipelPool



class ManagedPipeline():
    """Sequential pipeline of PipelPools"""
    pipes: List[PipelPool]
    queues: List[Queue] # queues[0] -> first module -> queues[1] -> second module -> queues[2] -> ...
        
    def __init__(self, pipes:List[PipelPool], in_queue=Queue(), out_queue=Queue()):
        self.pipes = pipes
        self.queues = []
        self.refresh_pipes()
    
    def put(self, *args, **kwargs):
        self.queues[0].put((args, kwargs))

    def get(self):
        return self.pipes[-1].get()
        
    def refresh_pipes(self, in_queue=Queue(), out_queue=Queue()):
        self.queues.clear()
        self.queues.append(in_queue)
        for i in range(len(self.pipes) - 1):
            self.pipes[i].in_queue = self.queues[-1]
            self.queues.append(Queue())
            self.pipes[i].out_queue = self.queues[-1]
            self.pipes[i].refresh(num_workers=1)
        self.pipes[-1].in_queue = self.queues[-1]
        self.queues.append(out_queue)
        self.pipes[-1].out_queue = self.queues[-1]
        self.pipes[-1].refresh(num_workers=1)
        return in_queue, out_queue
    
    def close(self):
        for pipe in self.pipes:
            pipe.close()
        
    
    def __update(self):
        pass
    
    def __len__(self):
        return len(self.pipes)


__all__ = [
    'ManagedPipeline'
]