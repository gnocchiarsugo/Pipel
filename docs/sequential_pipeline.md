# ConstrainedPipeline

`ConstrainedPipeline` is a base class for pipelines that enforce **type constraints** 
on their elements. Only `UnsafePipelineComponent` instances are allowed as pipeline elements.

It inherits from Python’s built-in `list` and `ABC` to combine:
- Sequence behavior
- Custom validation logic
- Abstract execution contract

---

## Features

### ✔️ Type-Safe Pipeline Elements

All elements must be of type `UnsafePipelineComponent`.  
This is validated at:

- initialization (`__init__`)
- `append` / `extend`
- `__setitem__`
- list operations (`+`, `+=`, `*`, `*=`)

Attempting to insert a different type raises a `ValueError`.

---

### ✔️ Immutable Sorting

`sort()` is explicitly **disabled** to maintain pipeline order:

```python
pipeline.sort()  # Raises NotImplementedError
```

---

### ✔️ Copying

`copy()` returns a new `ConstrainedPipeline` instance containing the same elements:

```python
pipeline_copy = pipeline.copy()
```

---

### ✔️ String Representation

- `__str__()` → readable list of elements
- `__repr__()` → readable list + length info

```python
print(pipeline)
print(repr(pipeline))
```

---

## Constructor

```python
ConstrainedPipeline(pipes: List[UnsafePipelineComponent])
```

- Validates that `pipes` are all `UnsafePipelineComponent` instances
- Initializes a list with the validated pipes

---

## Abstract Method

### `run(self, *args, **kwargs)`

Subclasses **must implement** `run()` to define pipeline execution logic.

```python
def run(self, *args, **kwargs):
    raise NotImplementedError
```

---

# SequentialPipeline

`SequentialPipeline` is a concrete subclass of `ConstrainedPipeline`.  
It executes its pipeline components **sequentially** on the provided data.

---

## Run Method

```python
def run(self, data: PipelData, exec_mode: EXEC_MODE = 'sync') -> PipelData:
    _data: PipelData = data
    for pipe in self:
        _data = pipe(_data, exec_mode=exec_mode)
    return _data
```

- Iterates through each component in order  
- Calls each component with the provided `exec_mode` (`'sync'` or `'async'`)  
- Returns the final processed `PipelData` object

---

## Usage Example

```python
from pipel import PipelData, SequentialPipeline, UnsafePipelineComponent

class Adder(UnsafePipelineComponent):
    
    def _run(self, x):
        return PipelData(args=(x + 2,))
    
    async def _a_run(self, x):
        return PipelData(args=(x * 2,))
    
# Define a SequentialPipeline
pipeline = SequentialPipeline([Adder(), Adder(), Adder()])

# Run synchronously
result = pipeline.run(PipelData(args=(4,)))

# Run asynchronously
res = pipeline.run(PipelData(args=(4,)), exec_mode='async')
```

---

## When to Use

- `ConstrainedPipeline` → when you need a type-safe, ordered pipeline of `UnsafePipelineComponent`s  
- `SequentialPipeline` → when you want to process data through multiple components **in sequence**

