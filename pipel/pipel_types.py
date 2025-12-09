from typing import Literal, Tuple, Any, Dict
from dataclasses import dataclass, field

EXEC_MODE = Literal['sync', 'async']

@dataclass
class PipelData:
    args: Tuple[Any]
    kwargs: Dict[str, Any] = field(default_factory=dict)
    
    # Needed for caching
    def __hash__(self):
        # convert dict → sorted tuple to make it hashable
        return hash((self.args, tuple(sorted(self.kwargs.items()))))
    
    # def __add__(self, other):
    #     """PipelData merging. If the kwargs has conflict, other rewrites.

    #     Args:
    #         other (PipelData): Other data to merge

    #     Returns:
    #         PipelData: Merged data
    #     """
    #     updated_kwargs = self.kwargs.copy()
    #     updated_kwargs.update(other.kwargs)        
    #     return PipelData(
    #         args=self.args + other.args,
    #         kwargs=updated_kwargs
    #     )
        
    # def __iadd__(self, other):
    #     merged: PipelData = self + other
    #     self.args = merged.args
    #     self.kwargs = merged.kwargs
    #     return self

        


__all__ = [
    'EXEC_MODE', 
    'PipelData'
]
