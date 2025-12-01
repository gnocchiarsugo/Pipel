from abc import ABC, abstractmethod
from multiprocessing import Queue, Process
from typing import Any, List, Dict, Tuple
import uuid

from ..pipel_types import EXEC_MODE

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
    
class PipelPool:
    component: PicklablePipelineComponent
    len_workers: int
    in_queue: Queue
    workers: List[Process]
    out_queue: Queue
    
    def __init__(self, component:PicklablePipelineComponent, *args, **kwargs):
        self.component = component
        self.len_workers = kwargs.get('init_workers') or 5
        self.out_queue = kwargs.get('out_queue') or Queue()
        
        self.in_queue = Queue()
        self.workers = []
        
        for worker_id in range(self.len_workers):
            proc = Process(target=self._pool_connector, 
                           args=(self.component, self.in_queue, self.out_queue, worker_id)
                    )
            proc.start()
            self.workers.append(proc)
            
    @staticmethod
    def _pool_connector(func, in_queue, out_queue, worker_id):
        while True:
            job = in_queue.get()
            __input_tup, __input_dic = job
            tup, dic = func(*__input_tup, **__input_dic)
            out_queue.put((worker_id, tup, dic))
        
    def submit(self, *args, **kwargs) -> None:
        self.in_queue.put((args, kwargs))  
        
    def get(self) -> Tuple[int, Tuple[Any], Dict[str, Any]]:
        """
            Returns one output from the queue
        """
        return self.out_queue.get()
    
    # Be careful when closing while running, the results are discarded
    # After implementing the Manager, report all results to it
    def close(self):
        for worker_id in range(self.len_workers):
            # self.input_queues[worker_id].put('STOP_TOKEN')
            self.workers[worker_id].terminate()
        latch:int = self.len_workers
        while latch > 0:
            for worker in self.workers:
                if not worker.is_alive():
                    latch -= 1

        
__all__ = [
    'PicklablePipelineComponent',
    'PipelPool'
]