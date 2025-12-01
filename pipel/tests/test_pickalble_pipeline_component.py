import pytest
from pipel.multiprocessing import PicklablePipelineComponent

class Adder(PicklablePipelineComponent):
    
    def __eq__(self, value):
        return True if isinstance(value, Adder) else False
    
    def _run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}
    

@pytest.fixture
def simple_component() -> Adder:
    return Adder()

def test_picklable_component_init(simple_component):
    assert simple_component is not None
    
def test_picklable_component_run(simple_component):
    res, _ = simple_component(10)
    assert res[0] == 12
