from abc import ABC, abstractmethod
from typing import Any, Optional
from logging import Logger
from .pipel_types import EXEC_MODE
from functools import lru_cache, wraps
import uuid

class UnsafePipelineComponent(ABC):
    """
        Pipeline component used for quick prototyping
    """
    logger: Optional[Logger]
    cache_size: Optional[int]
    
    @staticmethod
    def __identity_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tup, dic = func(*args, **kwargs)
            return tup, dic
        return wrapper
            
    
    def __init__(self, *args, **kwargs):
        self.logger = kwargs.get('logger')
        self.cache_size = kwargs.get('cache_size') or 0
        self.id = uuid.uuid4().hex  
        self.__logger_decorator = kwargs.get('logger_decorator') or self.__identity_decorator
        
        @lru_cache(maxsize=self.cache_size)
        @self.__logger_decorator
        def __cached_run(*args, **kwargs) -> Any:
            return self._run(*args, **kwargs)
        
        @lru_cache(maxsize=self.cache_size)
        @self.__logger_decorator
        async def __cached_a_run(*args, **kwargs) -> Any:
            return await self._a_run(*args, **kwargs)
        
        # The only exposed API to run the pipe is __call__
        self.__run = __cached_run
        self.__a_run = __cached_a_run

    def __call__(self, *args, exec_mode:EXEC_MODE='sync', **kwargs):
        if exec_mode == 'sync':
            return self.__run(*args, **kwargs)
        elif exec_mode == 'async':
            return self.__a_run(*args, **kwargs)
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
        return f'{self.__class__.__name__}(id={self.id}, logger={repr(self.logger)}, cache_size={self.cache_size})'
        
    def cache_info(self, *args, exec_mode:EXEC_MODE='sync', **kwargs):
        if exec_mode == 'sync':
            return self.__run.cache_info()
        elif exec_mode == 'async':
            return self.__a_run.cache_info()
        else:
            raise ValueError('exec_mode must be either \'sync\' or \'async\'.')
        
    def cache_clear(self, *args, exec_mode:EXEC_MODE='sync', **kwargs):
        if exec_mode == 'sync':
            return self.__run.cache_clear()
        elif exec_mode == 'async':
            return self.__a_run.cache_clear()
        else:
            raise ValueError('exec_mode must be either \'sync\' or \'async\'.')        

class PipelineComponent(UnsafePipelineComponent):
    """
        The OG component
    """
    
    def __init__(self, *args, **kwargs):

        def logger_validation_decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                self.logger.info(f'Validating input to {repr(self)}') if self.logger is not None else None
                self.validate_input(*args, **kwargs)
                self.logger.info('Valid input') if self.logger is not None else None
                self.logger.info(f'Running {repr(self)}') if self.logger else None
                tup, dic = func(*args, **kwargs)
                self.logger.info(f'Finished running {repr(self)}') if self.logger else None
                self.logger.info('Validating output') if self.logger is not None else None
                self.validate_output(*tup, **dic)
                self.logger.info(f'Valid output from {repr(self)}') if self.logger is not None else None
                return tup, dic
            return wrapper 
        super().__init__(*args, logger_decorator=logger_validation_decorator, **kwargs)
    
    @abstractmethod
    def validate_input(self, *args, **kwargs):
        assert 1 != 1

    @abstractmethod
    def validate_output(self, *args, **kwargs):
        assert 1 != 1
        
        
        
        
        
__all__ = [
    'UnsafePipelineComponent',
    'PipelineComponent'
]