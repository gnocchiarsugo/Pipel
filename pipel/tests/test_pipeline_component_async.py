from typing import Tuple
import pytest
import asyncio
from pipel import UnsafePipelineComponent, PipelData
from functools import _CacheInfo

class Adder(UnsafePipelineComponent):
    
    def _run(self, x):
        return PipelData(
            args = (x + 2,)
        )

class AsyncAdder(UnsafePipelineComponent):
    
    def _run(self, x):
        return PipelData(
            args = (x + 2,)
        )
    
    async def _a_run(self, x):
        return PipelData(
            args = (x * 2,)
        )

def test_pipeline_component_async_fallback():
    input_data = PipelData(args=(3,))
    comp = Adder()
    res:PipelData = asyncio.run(comp(input_data, exec_mode='async'))
    assert res.args[0] == input_data.args[0] + 2
    
def test_pipeline_component_async():
    input_data = PipelData(args=(3,))
    comp = AsyncAdder()
    res:PipelData = asyncio.run(comp(input_data, exec_mode='async'))
    assert res.args[0] == input_data.args[0] * 2
        