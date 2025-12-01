from abc import ABC, abstractmethod
from multiprocessing import Queue, Process
from typing import Any, List, Dict, Optional
from typing_extensions import override
from logging import Logger
import uuid

from .pipel_types import EXEC_MODE

class PicklablePipelineComponent(ABC):

    def __init__(self, *args, **kwargs):
        self.id = uuid.uuid4().hex  

    def __call__(self, *args, exec_mode:EXEC_MODE='sync', **kwargs):
        if exec_mode == 'sync':
            return self._run(*args, **kwargs)
        elif exec_mode == 'async':
            return self._a_run(*args, **kwargs)
        else:
            raise ValueError('exec_mode must be either \'sync\' or \'async\'.')
    
    @abstractmethod
    def _run(self, *args, **kwargs) -> Any:
        """
            Method implemented by the end User
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the _run method"
        )

    @abstractmethod
    async def _a_run(self, *args, **kwargs) -> Any:
        """
            Method implemented by the end User
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the _a_run method"
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(id={self.id})'

    def deepcopy(self):
        """
            Returns a new instance of the same derivative class.
        """
        return self.__class__()

class PipelWorker(Process):
    """
        Simple wrapper to Picka for working in parallel
    """
    # Component behaviour to run
    component: PicklablePipelineComponent
    worker_id: int
    # Input queue
    input_queue: Queue
    # Intake queue
    output_queue: Queue
    # Intake queue
    status_queue: Queue
    
    def __init__(self, 
                component:PicklablePipelineComponent, 
                worker_id: int,
                input_queue: Queue,       
                output_queue: Queue,       
                status_queue: Queue       
            ):
        super().__init__()
        self.component = component
        self.worker_id = worker_id
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.status_queue = status_queue
        
        # from inheritance
        # self._closed = False
        # self._popen = None
        
    @override
    def run(self) -> None:
        """
            Process Loop.
            Returns True when is successfully stopped
        """
        
        while True:
            self.status_queue.put((self.worker_id, "available"))
            job = self.input_queue.get()
            if job == "STOP_TOKEN":
                break
            __input_tup, __input_dic = job
            
            tup, dic = self.component(*__input_tup, **__input_dic)
            self.output_queue.put((self.worker_id, tup, dic))
        self.output_queue.put((self.worker_id, 'STOPPED'))
        # self._closed = True
        # self._popen = False
            
class PipelPool(PicklablePipelineComponent):
    component: PicklablePipelineComponent
    len_workers: int
    input_queues: List[Queue]
    workers: List[PipelWorker]
    output_queue: Queue
    status_queue: Queue
    available: Dict[int, bool]
    logger: Optional[Logger]
    
    def __init__(self, component:PicklablePipelineComponent, *args, **kwargs):
        super().__init__(*args, cache_size=0, **kwargs)  
        self.component = component
        self.len_workers = kwargs.get('init_workers') or 5
        self.input_queues = kwargs.get('input_queues') or [Queue() for _ in range(self.len_workers)]
        self.output_queue = kwargs.get('output_queue') or Queue()
        self.status_queue = kwargs.get('status_queue') or Queue()
        self.logger = kwargs.get('logger') or None
        
        self.workers = []
        for worker_id in range(self.len_workers):
            w = PipelWorker(
                    self.component, 
                    worker_id,
                    self.input_queues[worker_id], 
                    self.output_queue,
                    self.status_queue
                )
            w.start()
            self.workers.append(w)
        self.available = {i: False for i in range(self.len_workers)} 
        
    # Run logic
    
    @override
    def _run(self, *args, **kwargs):
        worker_id, _ = self.status_queue.get() #block=True, timeout=None
        self.input_queues[worker_id].put((args, kwargs))
        self.available[worker_id] = False
        _, tup, dic = self.output_queue.get()
        self.close()
        return tup, dic 
    
    @override
    def _a_run(self, *args, **kwargs):
        worker_id, _ = self.status_queue.get() #block=True, timeout=None
        self.input_queues[worker_id].put((args, kwargs))
        self.available[worker_id] = False
        _, tup, dic = self.output_queue.get()
        self.close()
        return tup, dic 
    
    # Be careful when closing while running, the results are discarded
    # After implementing the Manager, report all results to it
    # and filter for 'STOPPED'
    def close(self):
        for worker_id in range(self.len_workers):
            self.input_queues[worker_id].put('STOP_TOKEN')
        latch:int = self.len_workers
        while latch > 0:
            worker_id, status = self.output_queue.get()
            # There is no possiblity of worker sending multipler 'STOPPED'
            if status == 'STOPPED':
                latch -= 1
        

        
__all__ = [
    'PicklablePipelineComponent',
    'PipelWorker',
    'PipelPool'
]