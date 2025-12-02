from abc import ABC, abstractmethod
from multiprocessing import Queue, Process
import queue 
from typing import Any, List, Dict, Tuple
import uuid

from ..pipel_types import EXEC_MODE

class PicklablePipelineComponent(ABC):

    def __init__(self, *args, **kwargs):
        self.id = uuid.uuid4().hex  
        
    def __call__(self, *args, exec_mode:EXEC_MODE='sync', **kwargs):
        if exec_mode == 'sync':
            return self._run(*args, **kwargs)
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
    out_queue: Queue
    event_queue: Queue
    workers: List[Process]
    
    def __init__(self, component:PicklablePipelineComponent, *args, **kwargs):
        self.__num_workers = kwargs.get('num_workers') or 5
        self.component = component
        self.out_queue = kwargs.get('out_queue') or Queue()
        self.event_queue = kwargs.get('event_queue') or Queue()
        
        self.in_queue = Queue()
        self.workers = []
        
        self.__init_workers()
            
    @staticmethod
    def _pool_connector(func, in_queue: Queue, out_queue:Queue, event_queue:Queue):
        while True:
            try:
                job = in_queue.get(timeout=1)
                __input_tup, __input_dic = job
                tup, dic = func(*__input_tup, **__input_dic)
                out_queue.put((tup, dic))
            except queue.Empty:
                pass
            try:
                event_queue.get(block=False)
                break
            except queue.Empty:
                pass
            
        
    def put(self, *args, **kwargs) -> None:
        self.in_queue.put((args, kwargs))  
        
    def get(self) -> Tuple[Tuple[Any], Dict[str, Any]]:
        """
            Returns one output from the queue
        """
        return self.out_queue.get()
    
    def __init_workers(self) -> None:
        for _ in range(self.__num_workers):
            proc = Process(target=self._pool_connector, 
                           args=(self.component, self.in_queue, self.out_queue, self.event_queue)
                    )
            proc.start()
            self.workers.append(proc)

    def refresh(self, num_workers=0) -> Tuple[Queue, Queue]:
        """
            Closes the Pool, clears the workers list and reinits them
            Returns the input and output queues
        """
        self.close()
        self.workers.clear()
        self.__num_workers = num_workers
        self.__init_workers()
        return self.in_queue, self.out_queue

    def close(self):
        self.remove_workers(self.__num_workers)

    def remove_workers(self, amount:int):
        """Terminates the workers gracefully"""
        amount = min(self.__num_workers, amount)
        for _ in range(amount):
            self.event_queue.put(None)
            
        latch:int = amount
        while latch > 0:
            for i, worker in enumerate(self.workers):
                if not worker.is_alive():
                    self.workers.pop(i)
                    latch -= 1
        self.__num_workers -= amount

    def add_workers(self, amount:int):
        if amount <= 0:
            raise ValueError(f'Amount must be positive. Found {amount}')
        for _ in range(amount):
            proc = Process(target=self._pool_connector, 
                args=(self.component, self.in_queue, self.out_queue, self.event_queue)
                    )
            proc.start()
            self.workers.append(proc)
        self.__num_workers += amount

    def __len__(self):
        return self.__num_workers
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

        
__all__ = [
    'PicklablePipelineComponent',
    'PipelPool'
]