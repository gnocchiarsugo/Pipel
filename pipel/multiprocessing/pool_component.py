from abc import ABC, abstractmethod
from multiprocessing import Queue, Process
import queue 
from typing import List, Optional
import uuid

from ..pipel_types import PipelData

class PicklablePipelineComponent(ABC):

    def __init__(self):
        self.id = uuid.uuid4().hex  
        
    def __call__(self, data: PipelData):
        return self._run(*data.args, **data.kwargs)
    
    @abstractmethod
    def _run(self, *args, **kwargs) -> PipelData:
        """Method implemented by the end User"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the _run method"
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(id={self.id})'

    def deepcopy(self):
        """Returns a new instance of the same derivative class."""
        return self.__class__()
    
class PipelPool:
    # PipelPool can only have one internal managed Queue, this happens at __init__ time
    # we need to know if at __init__ the in_queue or out_queue lifetimes are internally or externally managed
    _in_queue_external_init: bool
    _out_queue_external_init: bool
    
    # Time in sec for job listening on any in_queue
    _job_timeout: float
    
    # The component to run
    component: PicklablePipelineComponent
    
    # Edges of the graph: incoming and outgoing
    # When runing all incoming are listened for __job_timeout
    # When the input is processed it's posted on all output queues
    in_queues: List[Queue]
    out_queues: List[Queue]

    # Queue used to send stop messages to the workers, may be used for more
    _event_queue: Queue
    
    # Stop worker signal
    STOP_TOKEN: str = 'STOP_TOKEN'
    
    # List of workers
    workers: List[Process]
        
    def __init__(
            self, 
            component: PicklablePipelineComponent,
            num_workers: int = 1,
            job_timeout: float = 1,
            in_queues: Optional[List[Queue]] = None,
            out_queues: Optional[List[Queue]] = None,
            event_queue: Optional[Queue] = None,
        ):
        self._job_timeout = job_timeout
        self.component = component
        
        self.event_queue = event_queue or Queue()
        self._init_data_queues(
            in_queues=in_queues, 
            out_queues=out_queues
        )
        
        self.workers = []
        self._add_workers(num_workers)
    
    
    @staticmethod
    def _pool_connector(
        func,
        in_queues: List[Queue],
        out_queues: List[Queue],
        event_queue: Queue,
        job_timeout: float,
        stop_token: str
    ):
        terminate = False
        while not terminate:
            for in_queue in in_queues:
                try:
                    data: PipelData = in_queue.get(timeout=job_timeout)
                    out: PipelData = func(data)
                    # Post the output to all out_queues
                    for out_queue in out_queues:
                        out_queue.put(out)
                except queue.Empty:
                    pass
                try:
                    job = event_queue.get(block=False)
                    if job == stop_token:
                        terminate = True
                        break
                except queue.Empty:
                    pass
     
    def _add_workers(self, num_workers: int) -> None:
        for _ in range(num_workers):
            proc = Process(target=self._pool_connector, 
                            args=(
                                self.component.deepcopy(), 
                                self.in_queues,
                                self.out_queues,
                                self.event_queue,
                                self._job_timeout,
                                self.STOP_TOKEN
                            )
                    )
            proc.start()
            self.workers.append(proc)

    def _init_data_queues(
        self,
        in_queues: Optional[List[Queue]] = None,
        out_queues: Optional[List[Queue]] = None
    ):
        """Initializes or creates the data queues"""
        
        self.in_queues = []
        self.out_queues = []
        
        
        self.in_queues, self._in_queue_external_init = (in_queues, True) if in_queues else ([Queue()], False)
        # if in_queues:
        #     self.in_queues = in_queues
        #     self._in_queue_external_init = True
        # else:
        #     self.in_queues = [Queue()]
        #     self._in_queue_external_init = False
        self.out_queues, self._out_queue_external_init = (out_queues, True) if out_queues else ([Queue()], False)
        # if out_queues:
        #     self.out_queues = out_queues
        #     self._out_queue_external_init = True
        # else:
        #     self.out_queues = [Queue()]
        #     self._out_queue_external_init = False

    def _close_internal_data_queues(self):
        if not self._in_queue_external_init:
            self.in_queues[0].close()
            self.in_queues[0].join_thread()
        if not self._out_queue_external_init:
            self.out_queues[0].close()
            self.out_queues[0].join_thread()
            
    def _close_event_queue(self):
        self.event_queue.close()
        self.event_queue.join_thread()
        
    def put(self, data: PipelData, index:int = 0) -> None:
        """
        Add data to the specified in_queue

        Args:
            index (int, optional): Queue index in which put the data. Defaults to 0.
        """
        self.in_queues[index].put(data)  
        
    def get(self, index: int = 0) -> PipelData:
        """Gets data from the specified queue

        Args:
            index (int, optional): Queue index in which get the data from. Defaults to 0.

        Returns:
           PipelData: Output data
        """
        return self.out_queues[index].get()
    
    def change_component(self, 
                component: PicklablePipelineComponent,
                num_workers: int = 1,
                in_queues: Optional[List[Queue]] = None,
                out_queues: Optional[List[Queue]] = None,
                event_queue: Optional[Queue] = None,
                force: bool = False
            ) -> None:
        self.remove_workers(len(self), force=force)
        # self._close_internal_data_queues()
        # self._close_event_queue()
        
        self.component = component
        
        # self._init_data_queues(
        #     in_queues=in_queues,
        #     out_queues=out_queues
        # )
        # self.event_queue = event_queue or Queue()
        
        if out_queues:
            self.out_queues = out_queues
        if in_queues:
            self.in_queues = in_queues
        if event_queue:
            self.event_queue = event_queue
        
        self._add_workers(num_workers)

    def close(self, force: bool = False):
        if len(self):
            self.remove_workers(len(self), force=force)
        self._close_internal_data_queues()
        self._close_event_queue()

    def remove_workers(self, amount: int, force: bool = False):
        if amount <= 0:
            raise ValueError(f'Amount must be strictly positive. Found {amount}')
        
        amount = min(len(self.workers), amount)
        for _ in range(amount):
            self.event_queue.put(self.STOP_TOKEN)
            
        if force:
            for i in range(amount):
                self.workers[i].kill()
        else:
            # Wait for the workers to terminate
            latch: int = amount
            while latch > 0:
                for i, proc in enumerate(self.workers):
                    proc.join(timeout=self._job_timeout * 2)
                    if not proc.is_alive():
                        latch -= 1

        self.workers = [worker for worker in self.workers if worker.is_alive()]

    def add_workers(self, amount: int):
        # Validate amount
        if amount <= 0:
            raise ValueError(f'Amount must be strictly positive. Found {amount}')
        
        self._add_workers(amount)

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