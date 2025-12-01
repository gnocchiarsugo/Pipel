from abc import ABC, abstractmethod
from typing import List, Any, Tuple, Dict
from typing_extensions import override
from .pipeline_component import PipelineComponent
from .pipel_types import EXEC_MODE

class ConstrainedPipeline(list, ABC):
    """
        Overrides fro list common to all pipelines
    """
    @staticmethod
    def __validate_input(input):
        _input = input if isinstance(input, list) else [input]
        for pipe in _input:
            if not isinstance(pipe, PipelineComponent):
                return ValueError('All pipe components must be of type PipelineComponent.')
    
    def __init__(self, pipes: List[PipelineComponent]):
        self.__validate_input(pipes)
        super().__init__(pipes)
    
    @override   
    def sort(self, *args, **kwargs) -> None:
        raise NotImplementedError("Sort function currently not implemented.")

    @override
    def __setitem__(self, key, item) -> None:
        self.__validate_input(item)
        super().__setitem__(key, item)
    
    @override
    def append(self, item) -> None:
        self.__validate_input(item)
        if isinstance(item, list):
            raise ValueError('Can only append PipeComponents not lists.')
        super().append(item)
    
    @override
    def extend(self, iterable) -> None:
        self.__validate_input(iterable)
        super().extend(iterable)
        
    @override
    def __add__(self, other):
        self.__validate_input(other)
        # result = super().__add__(other)
        return self.__class__(super().__add__(other))
    
    @override
    def __iadd__(self, value):
        self.__validate_input(value)
        # result = super().__iadd__(value)
        return self.__class__(super().__iadd__(value))
    
    @override
    def __str__(self) -> str:
        out = '['
        for pipe in self:
            out += f'{str(pipe)}, '
        out += ']'
        return out
    
    @override
    def __repr__(self) -> str:
        out = '[\n'
        for pipe in self:
            out += f'{repr(pipe)},\n'
        out += ']\n'
        out += f'Lenght: {len(self)}'
        return out
    
    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError(f'run() not implemented for {self.__class__.__name__}')

class SequentialPipeline(ConstrainedPipeline):

    def run(self, *args, exec_mode:EXEC_MODE='sync', **kwargs) -> Tuple[Tuple[Any], Dict[str, Any]]:
        _args: Tuple[Any] = args
        _kwargs: Dict[str, Any] = kwargs
        for pipe in self:
            _kwargs.update({'exec_mode':exec_mode})
            _args, _kwargs = pipe(*_args, **_kwargs)
        return _args, _kwargs

