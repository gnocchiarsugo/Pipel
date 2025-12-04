from abc import ABC, abstractmethod
from multiprocessing import Queue, Process
import queue 
import time
from typing import Any, List, Dict, Tuple
import uuid

from ..pipel_types import EXEC_MODE

class PicklablePipelineComponent(ABC):

    def __init__(self):
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
    __out_queue_external_init: bool
    __job_timeout: float
    component: PicklablePipelineComponent
    len_workers: int
    in_queue: Queue
    out_queue: Queue
    event_queue: Queue
    workers: List[Process]
    STOP_TOKEN:str = 'STOP_TOKEN'
        
    def __init__(
            self, component:PicklablePipelineComponent,
            num_workers = 1,
            job_timeout = 1,
            out_queue = None,
            event_queue = None,
        ):
        # Flag to know if out_queue lifetime is externally managed
        self.__out_queue_external_init = True if out_queue else False
        # seconds to wait for jobs and then to check if the worker should be deleted
        self.__job_timeout = job_timeout
        self.component = component
        self.event_queue = event_queue or Queue()
        self.out_queue = out_queue or Queue()
        self.in_queue = Queue()
        self.workers = []
        self.__init_workers(num_workers)
    
    @staticmethod
    def _pool_connector(
        func,
        in_queue: Queue,
        out_queue: Queue,
        event_queue: Queue,
        job_timeout: float,
        stop_token: str
    ):
        while True:
            try:
                job = in_queue.get(timeout=job_timeout)
                __input_tup, __input_dic = job
                tup, dic = func(*__input_tup, **__input_dic)
                out_queue.put((tup, dic))
            except queue.Empty:
                pass
            try:
                job = event_queue.get(block=False)
                if job == stop_token:
                    break
            except queue.Empty:
                pass
            time.sleep(0.01)
        
    def put(self, *args, **kwargs) -> None:
        self.in_queue.put((args, kwargs))  
        
    def get(self) -> Tuple[Tuple[Any], Dict[str, Any]]:
        """Returns one output from the queue"""
        return self.out_queue.get()
    
    def __init_workers(self, num_workers: int) -> None:
        for _ in range(num_workers):
            proc = Process(target=self._pool_connector, 
                           args=(self.component.deepcopy(), 
                                 self.in_queue,
                                 self.out_queue,
                                 self.event_queue,
                                 self.__job_timeout,
                                 self.STOP_TOKEN
                            )
                    )
            proc.start()
            self.workers.append(proc)

    def refresh(self, 
                num_workers = 1,
                in_queue = None,
                out_queue = None
            ) -> None:
        self.close()
        
        # refresh using an external out_queue or not
        self.__out_queue_external_init = True if out_queue else False
        self.out_queue = out_queue or Queue()
        self.in_queue = in_queue or Queue()
        self.event_queue = Queue()
        self.__init_workers(num_workers)

    def close(self, force:bool = False):
        self.remove_workers(len(self.workers) ,force=force)
        try:
            self.in_queue.close() 
            self.in_queue.join_thread()
            self.event_queue.close()
            self.event_queue.join_thread()
        
            if not self.__out_queue_external_init:
                self.out_queue.close()
                self.out_queue.join_thread()
        except:
            pass

    def remove_workers(self, amount:int, force:bool=False):
        """Terminates the workers gracefully"""
        amount = min(len(self.workers), amount)
        
        # Put amount times of Nones in the event queue
        # the signal is then taken by workers and self-terminate
        for _ in range(amount):
            self.event_queue.put(self.STOP_TOKEN)
            
        terminated = []
        if force:
            for i in range(amount):
                self.workers[i].kill()
                terminated.append(i)
        else:
            # Wait for the workers to terminate
            latch:int = amount
            while latch > 0:
                terminated = []
                for i, proc in enumerate(self.workers):
                    proc.join(timeout=self.__job_timeout + 1)
                    if not proc.is_alive():
                        terminated.append(i)
                        latch -= 1
                    else:
                        continue

        # Pop the terminated workers from the list
        self.workers = [worker for i, worker in enumerate(self.workers) if i not in terminated]

    def add_workers(self, amount:int):
        # Validate amount
        if amount <= 0:
            raise ValueError(f'Amount must be positive. Found {amount}')
        
        for _ in range(amount):
            proc = Process(target=self._pool_connector, 
                args=(
                    self.component.deepcopy(),
                    self.in_queue,
                    self.out_queue,
                    self.event_queue,
                    self.__job_timeout,
                    self.STOP_TOKEN
                )
            )
            proc.start()
            self.workers.append(proc)

    def __len__(self):
        return len(self.workers)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

        
__all__ = [
    'PicklablePipelineComponent',
    'PipelPool'
]