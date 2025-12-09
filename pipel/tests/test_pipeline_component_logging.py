import pytest
from pipel import UnsafePipelineComponent, PipelineComponent
from pipel import PipelData

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
    
class SafeAdder(PipelineComponent):
    
    def validate_input(self, data: PipelData):
        for d in data.args:
            assert isinstance(d, int)
        
    def validate_output(self, out_data: PipelData):
        for d in out_data.args:
            assert isinstance(d, int)
        
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

class CustomLogger():
    events: list
    
    def __init__(self):
        self.events = []
    
    def info(self, msg):
        self.events.append(msg)
        
    def __repr__(self):
        return 'CustomLogger'


@pytest.fixture
def simple_component() -> UnsafePipelineComponent:
    return Adder()


def test_pipeline_component_logger_init():
    logger = CustomLogger()
    adder = Adder(logger=logger)
    assert adder.logger is not None    

def test_pipeline_component_logger_events_existance():
    logger = CustomLogger()
    adder = Adder(logger=logger)
    assert adder.logger.events is not None    

def test_pipeline_component_logger_events():
    logger = CustomLogger()
    adder = SafeAdder(logger=logger)
    input_data = PipelData(
        args = (1,),
        kwargs = {}
    )
    adder(input_data)
    assert adder.logger.events == [
        f'Validating input to {repr(adder)}',
        'Valid input',
        f'Running {repr(adder)}',
        f'Finished running {repr(adder)}',
        'Validating output',
        f'Valid output from {repr(adder)}'
    ]
    
