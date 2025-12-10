from abc import ABC, abstractmethod
from typing import List
from typing_extensions import override
from .pipeline_component import UnsafePipelineComponent, PipelData
from .pipel_types import EXEC_MODE

class ConstrainedPipeline(list, ABC):

    @staticmethod
    def __validate_input(input):
        _input = input if isinstance(input, list) else [input]
        for pipe in _input:
            if not isinstance(pipe, UnsafePipelineComponent):
                return ValueError('All pipe components must be of type UnsafePipelineComponent.')
    
    def __init__(self, pipes: List[UnsafePipelineComponent]):
        self.__validate_input(pipes)
        super().__init__(pipes)
    
    @override
    def copy(self):
        """Returns a deepcopy"""
        return self.__class__(super().copy())
    
    @override
    def append(self, item) -> None:
        """Appends item of type UnsafePipelineComponent"""
        # self.__validate_input(item)
        if not isinstance(item, UnsafePipelineComponent):
            raise ValueError('Can only append UnsafePipelineComponent.')
        super().append(item)
    
    @override
    def extend(self, iterable) -> None:
        """Appends every elements in the iterable to the list"""
        self.__validate_input(iterable)
        super().extend(iterable)
    
    @override   
    def sort(self, *args, **kwargs) -> None:
        raise NotImplementedError("Sort function currently not implemented.")

    @override
    def __setitem__(self, key, item) -> None:
        if not isinstance(item, UnsafePipelineComponent):
            raise ValueError('Can only set UnsafePipelineComponent.')
        self.__validate_input(item)
        super().__setitem__(key, item)
    
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
    def __mul__(self, value):
        return self.__class__(super().__mul__(value))
    
    @override
    def __rmul__(self, value):
        return self.__class__(super().__rmul__(value))
    
    @override
    def __imul__(self, value):
        return self.__class__(super().__imul__(value))
    
    @override
    def __str__(self) -> str:
        out = '['
        for pipe in self:
            out += f'{str(pipe)}, '
        out += ']'
        return out
    
    @override
    def __repr__(self) -> str:
        out = '['
        for pipe in self:
            out += f'{repr(pipe)},'
        out += '] '
        out += f'Lenght: {len(self)}'
        return out
    
    @abstractmethod
    def run(self, *args, **kwargs):
        raise NotImplementedError(f'run() not implemented for {self.__class__.__name__}')

class SequentialPipeline(ConstrainedPipeline):

    def run(self, data: PipelData, exec_mode:EXEC_MODE = 'sync') -> PipelData:
        _data: PipelData = data
        for pipe in self:
            _data = pipe(_data, exec_mode=exec_mode)
        return _data

__all__ = [
    'ConstrainedPipeline',
    'SequentialPipeline'
]