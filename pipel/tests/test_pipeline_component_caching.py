from typing import Tuple
import pytest
from pipel import UnsafePipelineComponent
from pipel import PipelData
from functools import _CacheInfo

class Adder(UnsafePipelineComponent):
    
    def _run(self, x):
        return PipelData(
            args = (x + 2,),
            kwargs = {}
        )
    
    def _a_run(self, x):
        return PipelData(
            args = (x + 2,),
            kwargs = {}
        )

@pytest.fixture
def pytest_fixture() -> Tuple[UnsafePipelineComponent, PipelData]:
    return Adder(), PipelData(args=(10,), kwargs={})

def test_pipeline_component_init_caching(pytest_fixture):
    simple_component, input_data = pytest_fixture
    
    
    simple_component(input_data) # Miss
    info: _CacheInfo = simple_component.cache_info()
    assert info.maxsize == 0
    assert info.hits == 0
    assert info.currsize == 0
    assert info.misses == 1
    
def test_pipeline_component_zero_caching(pytest_fixture):
    # simple_component(1) # Miss
    # simple_component(1) # Miss
    simple_component, input_data = pytest_fixture
    
    simple_component(input_data) # Miss
    simple_component(input_data) # Miss
    
    info: _CacheInfo = simple_component.cache_info()
    assert info.maxsize == 0
    assert info.hits == 0 # Zero caching
    assert info.currsize == 0
    assert info.misses == 2
    
def test_pipeline_component_caching():
    adder = Adder(cache_size = 2)
    input_data1 = PipelData(args = (1,), kwargs = {})
    input_data2 = PipelData(args = (2,), kwargs = {})
    input_data3 = PipelData(args = (3,), kwargs = {})
    adder(input_data1) # Miss
    adder(input_data2) # Miss
    adder(input_data1) # Hit
    adder(input_data3) # Eviction of 2 in favor of 3
    info = adder.cache_info()
    assert info.maxsize == 2
    assert info.hits == 1
    assert info.currsize == 2
    assert info.misses == 3