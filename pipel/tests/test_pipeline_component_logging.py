import pytest
from pipel import UnsafePipelineComponent, PipelineComponent

class Adder(UnsafePipelineComponent):
    
    def _run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
class SafeAdder(PipelineComponent):
    
    def validate_input(self, *args, **kwargs):
        assert 1 == 1
        
    def validate_output(self, *args, **kwargs):
        assert 1 == 1
        
    def _run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}

class CustomLogger():
    events: list
    
    def __init__(self):
        self.events = []
    
    def info(self, msg, *args, **kwargs):
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
    adder(1)
    assert adder.logger.events == [
        f'Validating input to {repr(adder)}',
        'Valid input',
        f'Running {repr(adder)}',
        f'Finished running {repr(adder)}',
        'Validating output',
        f'Valid output from {repr(adder)}'
    ]
    
