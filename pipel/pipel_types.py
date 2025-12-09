from typing import Literal, Tuple, Any, Dict
from dataclasses import dataclass, field

EXEC_MODE = Literal['sync', 'async']

@dataclass
class PipelData:
    args: Tuple[Any]
    kwargs: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        # convert dict → sorted tuple to make it hashable
        return hash((self.args, tuple(sorted(self.kwargs.items()))))


__all__ = [
    'EXEC_MODE', 
    'PipelData'
]