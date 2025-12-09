import pytest
from pipel import PipelineComponent
from pipel import PipelData


class Adder(PipelineComponent):
    input_data: PipelData
    
    def validate_input(self, data: PipelData):
        self.input_data = data
        if data.args[0] < 1:
            raise ValueError("Input only > 1 allowed")
    
    def validate_output(self, out_data: PipelData):
        if out_data.args[0] != self.input_data.args[0] + 2:
            raise ValueError("Error: Adder was supposed to +2 the input")
    
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
def simple_component() -> PipelineComponent:
    return Adder()


def test_pipeline_component_ingress_validation(simple_component):
    input_data = PipelData(args = (0,))
    # Denpending on the validation method it can be ValueError or AssertError etc.
    with pytest.raises(expected_exception=ValueError):
        simple_component(input_data)

def test_pipeline_component_exit_validation(simple_component):
    input_data = PipelData(args = (1,))
    out_data:PipelData = simple_component(input_data)
    assert out_data.args[0] == 3
    
