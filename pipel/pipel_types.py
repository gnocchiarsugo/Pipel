from typing import Literal, Tuple, Any, Dict
from dataclasses import dataclass

EXEC_MODE = Literal['sync', 'async']

@dataclass
class PipelData:
    args: Tuple[Any]
    kwargs: Dict[str, Any]


__all__ = [
    'EXEC_MODE', 
    'PipelData'
]