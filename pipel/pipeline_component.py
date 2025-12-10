from abc import ABC, abstractmethod
from typing import Any, Optional
from .pipel_types import EXEC_MODE
from functools import lru_cache, wraps
import uuid
import types
from .pipel_types import PipelData

class UnsafePipelineComponent(ABC):
    """Pipeline component used for quick prototyping"""
    logger: Optional[Any]
    cache_size: Optional[int]
    
    @staticmethod
    def __identity_decorator(func):
        @wraps(func)
        def wrapper(data: PipelData):
            return func(data)
            
        return wrapper
            
    def __init__(
        self, *,
        logger = None,
        cache_size: int = 0,
        logger_decorator = None,
    ):
        self.logger = logger
        self.cache_size = cache_size
        self.id = uuid.uuid4().hex  
        self.__logger_decorator = logger_decorator or self.__identity_decorator
        
        # Cached run
        @lru_cache(maxsize=self.cache_size)
        @self.__logger_decorator
        def __cached_run(data: PipelData) -> Any:
            return self._run(*data.args, **data.kwargs)
        
        
        _a_run = getattr(self, '_a_run', None)
        if _a_run is None:
            async def _a_run(self, *args, **kwargs):
                return self._run(*args, **kwargs)
            setattr(self, '_a_run', types.MethodType(_a_run, self))
        
        
        # Cached a_run
        @lru_cache(maxsize=self.cache_size)
        @self.__logger_decorator
        async def __cached_a_run(data: PipelData) -> Any:
            return await self._a_run(*data.args, **data.kwargs)
        
        # The only exposed API to run the pipe is __call__
        self.__run = __cached_run
        self.__a_run = __cached_a_run

    def __call__(self, data: PipelData, exec_mode: EXEC_MODE = 'sync'):
        if exec_mode == 'sync':
            return self.__run(data)
        elif exec_mode == 'async':
            return self.__a_run(data)
        else:
            raise ValueError('exec_mode must be either \'sync\' or \'async\'.')
    
    @abstractmethod
    def _run(self, *args, **kwargs) -> PipelData:
        """
            Method implemented by the end User
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the _run method"
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(id={self.id}, logger={repr(self.logger)}, cache_size={self.cache_size})'
        
    def cache_info(self, exec_mode: EXEC_MODE = 'sync'):
        if exec_mode == 'sync':
            return self.__run.cache_info()
        elif exec_mode == 'async':
            return self.__a_run.cache_info()
        else:
            raise ValueError('exec_mode must be either \'sync\' or \'async\'.')
        
    def cache_clear(self, exec_mode: EXEC_MODE = 'sync'):
        if exec_mode == 'sync':
            return self.__run.cache_clear()
        elif exec_mode == 'async':
            return self.__a_run.cache_clear()
        else:
            raise ValueError('exec_mode must be either \'sync\' or \'async\'.')        

    def deepcopy(self):
        """
            Returns a new instance of the same derivative class.
        """
        return self.__class__(
            logger = self.logger,
            logger_decorator = self.__logger_decorator, 
            cache_size = self.cache_size,
        )

class PipelineComponent(UnsafePipelineComponent):
    """The OG component"""

    def __init__(self, *args, **kwargs):

        def logger_validation_decorator(func):
            @wraps(func)
            def wrapper(data: PipelData):
                self.logger.info(f'Validating input to {repr(self)}') if self.logger else None
                self.validate_input(data)
                self.logger.info('Valid input') if self.logger else None
                self.logger.info(f'Running {repr(self)}') if self.logger else None
                result_data: PipelData = func(data)
                self.logger.info(f'Finished running {repr(self)}') if self.logger else None
                self.logger.info('Validating output') if self.logger else None
                self.validate_output(result_data)
                self.logger.info(f'Valid output from {repr(self)}') if self.logger else None
                return result_data
            return wrapper 
        kwargs.update({'logger_decorator': logger_validation_decorator})
        super().__init__(*args, **kwargs)
    
    @abstractmethod
    def validate_input(self, data: PipelData):
        assert 1 != 1

    @abstractmethod
    def validate_output(self, data: PipelData):
        assert 1 != 1
        
        
__all__ = [
    'UnsafePipelineComponent',
    'PipelineComponent'
]