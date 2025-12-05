from typing import List, Optional
import time
from multiprocessing import Queue, Process, Event
from multiprocessing.synchronize import Event as Event_t
from threading import Thread

from .pool_component import PipelPool
from ..pipel_types import PipelData

class ManagedPipeline:
    """Sequential pipeline of PipelPools"""
    pipe_pools: List[PipelPool]
    queues: List[Queue] # queues[0] -> first module -> queues[1] -> second module -> queues[2] -> ...
    _autoscaler: Process
    
    # Signals the autoscaler to termiante
    _autoscaler_event: Event_t
    
    _out_queue_external_init: bool
    _in_queue_external_init: bool
    
    autoscaling_bottomline: List[Optional[int]]
    autoscaling_upperline: List[Optional[int]]
    
    def __init__(
        self, 
        pipe_pools: List[PipelPool],
        in_queue: Optional[Queue] = None,
        out_queue: Optional[Queue] = None
    ):
        self._in_queue_external_init = True if in_queue else False
        in_queue = in_queue or Queue()
        self._out_queue_external_init = True if out_queue else False
        out_queue = out_queue or Queue()
        self.pipe_pools = pipe_pools
        self.queues = []
        self._init_queues(
            in_queue = in_queue,
            out_queue = out_queue
        )
        
        # Autoscaler init
        self._autoscaler = None
        self._autoscaler_event = None
        self.autoscaling_bottomline = [1] * len(self)
        self.autoscaling_upperline = [None] * len(self)
       
    def _init_queues(
        self,
        in_queue: Optional[List[Queue]] = None,
        out_queue: Optional[List[Queue]] = None,
    ):
        self.queues.append(in_queue)
        for i in range(len(self.pipe_pools) - 1):
            self.queues.append(Queue())
            self.pipe_pools[i].refresh(
                in_queues = [self.queues[-2]], 
                out_queues = [self.queues[-1]]
            )
        self.queues.append(out_queue)
        self.pipe_pools[-1].refresh(
            in_queues = [self.queues[-2]],
            out_queues = [self.queues[-1]]
        )
        
    def _close_queues(self):
        for q in self.queues[1:-1]:
            q.close()
            q.join_thread()
            
        if not self._in_queue_external_init:
            self.queues[0].close()
            self.queues[0].join_thread()
        if not self._out_queue_external_init:
            self.queues[-1].close()
            self.queues[-1].join_thread()
        
    def put(self, data: PipelData):
        self.queues[0].put(data)

    def get(self) -> PipelData:
        return self.pipe_pools[-1].get()
 
    def refresh_pipes(self):
        for pipe in self.pipe_pools:
            pipe.refresh()
    
    def close(self):
        if self._autoscaler_event:
            self.close_autoscaling()
        for pipe in self.pipe_pools:
            pipe.close()
        self._close_queues()
            
    # Add worker API
    def add_worker(self, component_index: int, amount:int = 1):
        self.pipe_pools[component_index].add_workers(amount)
    
    # Remove worker API
    def remove_worker(self, component_index: int, amount:int = 1):
        self.pipe_pools[component_index].remove_workers(amount)
    
    def start_autoscaling(self, 
                          scaleup_cond: str, 
                          scaledown_cond: str,
                          update_every: int = 1,
                          bottom_line: List[Optional[int]] = [None],
                          upper_line: List[Optional[int]] = [None]
                        ):
        if bottom_line[0]:
            self.autoscaling_bottomline = bottom_line
        if upper_line[0]:
            self.autoscaling_upperline = upper_line
        assert len(self.autoscaling_bottomline) == len(self), "Size of bottom_line must be the same as the number of pipes"
        assert len(self.autoscaling_upperline) == len(self), "Size of upper_line must be the same as the number of pipes"
        
        # Register the bottom lines, they dont exist if start_autoscaling is not called
        self._autoscaler_event = Event()
        self._autoscaler = Thread(
            target = self.__autoscaling,
            args = (
                self.queues,
                self.pipe_pools,
                scaleup_cond,
                scaledown_cond,
                update_every,
                self._autoscaler_event,
                self.autoscaling_bottomline,
                self.autoscaling_upperline
            )
        )
        self._autoscaler.start()
    
    def close_autoscaling(self):
        # Sets the autoscaler termination event
        self._autoscaler_event.set()
        self._autoscaler_event = None
    
    @staticmethod
    def __autoscaling(
        queues: List[Queue], 
        pipes: List[PipelPool],
        scaleup_cond: List[bool],                    
        scaledown_cond: List[bool],                    
        update_every: float,
        termination_event: Event_t,
        bottomline: List[Optional[int]],
        upperline: List[Optional[int]],
    ):
        """
            If a PipelPool is init with a lower num_workers than its bottomline:
            1. That's on you
            2. Workers are added only if the upscale condition is triggered
            3. Even if the downscale is triggered, the num_workers will not be decreased
        """
        safe_globals = {
                "__builtins__": {
                    "all": all,
                    "any": any,
                }
        }
        while True:
            # Termination check
            if termination_event.is_set():
                break
            # The last queue is the output queue
            safe_locals = {"qsize": [q.qsize() for q in queues[:-1]]}   
            up_flag = eval(scaleup_cond, safe_globals, safe_locals)
            down_flag = eval(scaledown_cond, safe_globals, safe_locals)
            for i, (up, down) in enumerate(zip(up_flag, down_flag)):
                if up == down == True:
                    # Contradictory signal: Do nothing
                    # TODO: Log the error
                    continue
                else:
                    if up:
                        if upperline[i]:
                            if len(pipes[i]) + 1 <= upperline[i]:
                                print('added')
                                pipes[i].add_workers(1)
                        else:
                            pipes[i].add_workers(1)
                            
                    if down:
                        # Remove worker only if there are more than one
                        if len(pipes[i]) - 1 >= bottomline[i]:
                            pipes[i].remove_workers(1)
                            
            time.sleep(update_every)
      
    def is_autoscaling_running(self) -> bool:
        return self._autoscaler.is_alive()
                        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __len__(self):
        return len(self.pipe_pools)


__all__ = [
    'ManagedPipeline'
]