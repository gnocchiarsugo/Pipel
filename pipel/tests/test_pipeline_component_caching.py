import pytest
from pipel import UnsafePipelineComponent

class Adder(UnsafePipelineComponent):
    
    def _run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}

@pytest.fixture
def simple_component() -> UnsafePipelineComponent:
    return Adder()



def test_pipeline_component_init_caching(simple_component):
    simple_component(1) # Miss
    info = simple_component.cache_info()
    assert info.maxsize == 0
    assert info.hits == 0
    assert info.currsize == 0
    assert info.misses == 1
    
def test_pipeline_component_zero_caching(simple_component):
    simple_component(1) # Miss
    simple_component(1) # Miss
    info = simple_component.cache_info()
    assert info.maxsize == 0
    assert info.hits == 0 # Zero caching
    assert info.currsize == 0
    assert info.misses == 2
    
def test_pipeline_component_caching():
    adder = Adder(cache_size=2)
    adder(1) # Miss
    adder(2) # Miss
    adder(1) # Hit
    adder(3) # Eviction of 2 in favor of 3
    info = adder.cache_info()
    assert info.maxsize == 2
    assert info.hits == 1
    assert info.currsize == 2
    assert info.misses == 3