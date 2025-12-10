# PipelData
PipelData is the Pythonic dataclass used to comunicate between Pipeline components.

## Implementation
```python
@dataclass
class PipelData:
    args: Tuple[Any]
    kwargs: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash((self.args, tuple(sorted(self.kwargs.items()))))
```
`args` and `kwargs` are exrtacted and passed at runtime to the components.  
The `__hash__` method is required for caching.

## Features
1. Since the argument are passed to the components as:
    ```python
    component(*data.args, **data.kwargs)
    ```
    you can implement the _run function of the component as any other function:
    ```python
    def _run(self, x, y, value: int = 1):
    ```
    **Note:**   
    The number of `args` passed to the component must be equal to the number of `args` expected by the function. Also if `kwargs` is different from the default dictionary, then the component is expected to also handle keyword arguments.
2. Since `kwargs` is optional you can define `PipelData` simply with:
    ```python
    data = PipelData(
        args = (arg1, arg2, ),
    )
    ```
    the `kwargs` of `data` is the empty dictionary
3. By overriding the `__hash__` class method, it's possible to cache the class.

    







## Usage
```python
data = PipelData(
    args = (arg1, arg2, ),
    kwargs = {
        'k1': v1,
        'k2': v2
    }
)
```

