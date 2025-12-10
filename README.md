# Pipel - A Modular Data Pipeline Framework

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## Features

- **No Dependencies** – There are no dependencies to download
- **Validation** – Every step of the Pipeline requires a validation step, both in input and output
- **Modular** – I would be doing something wrong if it wasn't modular

---

## Quick Start

```python
from pipel import SequentialPipeline, UnsafePipelineComponent, PipelData

class Adder(UnsafePipelineComponent):
    
    def _run(self, x):
        return PipelData(args=(x + 2,))
    
    async def _a_run(self, x):
        return PipelData(args=(x + 2,))
    
class Multiplier(UnsafePipelineComponent):

    def _run(self, x):
        return PipelData(args=(x * 10,))
    
    async def _a_run(self, x):
        return PipelData(args=(x * 10,))

pipeline = SequentialPipeline([Adder(), Multiplier()])
input_data = PipelData(args=(1,))
res: PipelData = pipeline.run(input_data)
print(f'Output = {res.args[0]}')
```

## Usage Examples

### Initialization

```python
from pipel import SequentialPipeline, UnsafePipelineComponent

class Adder(UnsafePipelineComponent):
    
    def _run(self, x):
        return (x + 2,), {}
    
    async def _a_run(self, x):
        return (x + 2,), {}
    
class Multiplier(UnsafePipelineComponent):

    def _run(self, x):
        return (x * 10,), {}
    
    async def _a_run(self, x):
        return (x * 10,), {}

pipeline = SequentialPipeline([Adder(), Multiplier()])
```

### Pipeline Extension and Reduction
`SequentialPipeline` is list at heart, so you can:
- Append
- Insert
- Pop
- And all others Python list methods  

Comparisons between Pipes is manged by the `__eq__` method of your PipelineComponent.




