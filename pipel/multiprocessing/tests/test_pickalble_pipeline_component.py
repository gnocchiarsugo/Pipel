import pytest
from pipel.multiprocessing import PicklablePipelineComponent
from pipel import PipelData

class Adder(PicklablePipelineComponent):
    
    def __eq__(self, value):
        return True if isinstance(value, Adder) else False
    
    def _run(self, x):
        return PipelData(args=(x+2,), kwargs={})
    

@pytest.fixture
def simple_component() -> Adder:
    return Adder()

def test_picklable_component_init(simple_component):
    assert simple_component is not None
    
def test_picklable_component_run(simple_component):
    data = PipelData(args=(10,), kwargs={})
    out:PipelData = simple_component(data)
    assert out.args[0] == 12
