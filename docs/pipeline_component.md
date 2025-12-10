# PipelineComponent

`PipelineComponent` is the primary, safe, and fully validated pipeline component
that extends the lightweight `UnsafePipelineComponent`.  
It adds **input/output validation**, structured logging, and a strict execution contract.

This component is intended to be the “official” production-ready building block for pipelines.

---

## Features

### ✔️ Automatic Input & Output Validation
Before execution:

1. `validate_input(data)` runs  
2. The main component logic (`_run`) executes  
3. `validate_output(result)` runs  

Both methods are abstract and **must be implemented** by the subclass.

---

### ✔️ Optional Structured Logging

If `logger` is provided, the component logs:

- validation start  
- input validation success  
- component execution start  
- component execution finish  
- output validation start  
- output validation success  

The decorator ensures logs appear in the correct order.

---

### ✔️ Validation/Logging Decorator

`PipelineComponent` injects a `logger_validation_decorator` into the parent
`UnsafePipelineComponent`, meaning:

- sync `_run`
- async `_a_run` (if defined)
- caching
- logging
- validation

all compose cleanly through decorator stacking.

---

## Constructor

```python
PipelineComponent(
    *,
    logger=None,
    cache_size=0,
    logger_decorator=logger_validation_decorator,
    **kwargs
)
```

The class automatically provides a validation + logging decorator.

---

## Abstract Methods

### `validate_input(self, data: PipelData)`
Check whether the input is valid.  
Raise an exception if invalid.

### `validate_output(self, data: PipelData)`
Check whether the output is valid.  
Raise an exception if invalid.

---

## Example Implementation

```python
from pipel import PipelineComponent, PipelData
    
class AddPositive(PipelineComponent):
    input_data: int

    def validate_input(self, data: PipelData):
        self.input_data = data.args[0]
        assert self.input_data >= 0, "Input must be non-negative"        

    def validate_output(self, data: PipelData):
        assert self.input_data + 1 == data.args[0], "Error during _run execution"

    def _run(self, x):
        return PipelData(args=(x + 1,))
```

Usage:

```python
component = AddPositive()
component(PipelData(args=(5,)))
```

---

## Execution Order

1. Component is called  
2. LRU cache checks  
3. Logging + validation decorator applied  
4. `validate_input(data)`  
5. `_run(*args, **kwargs)`  
6. `validate_output(result)`  
7. Return result  

This ensures correctness and observability.

---

## When to Use `PipelineComponent`

Use it when you need:

- strict validation guarantees  
- logging for debugging or monitoring  
- predictable, traceable component behavior  
- production-ready pipeline code  

Use `UnsafePipelineComponent` instead when:

- experimenting quickly  
- bypassing validation  
- creating temporary prototype components  


