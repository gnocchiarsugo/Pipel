import pytest
from pipel import PipelineComponent
from typing import Any

class Adder(PipelineComponent):
    input: Any
    
    def validate_input(self, input, *args, **kwargs):
        self.input = input
        if input < 1:
            raise ValueError("Input only > 1 allowed")
    
    def validate_output(self, output, *args, **kwargs):
        if output != self.input + 2:
            raise ValueError("Error: Adder was supposed to +2 the input")
    
    def _run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}

@pytest.fixture
def simple_component() -> PipelineComponent:
    return Adder()


def test_pipeline_component_ingress_validation(simple_component):
    try: 
        simple_component.run(0)
        assert 1 == 0
    except:
        assert 1 == 1

def test_pipeline_component_exit_validation(simple_component):
    input = 1
    args, _ = simple_component(input)
    assert args[0] == 3
    
