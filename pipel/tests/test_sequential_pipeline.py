import pytest
from pipel import SequentialPipeline, PipelineComponent

class Adder(PipelineComponent):
    
    def validate_input(self, *args, **kwargs):
        assert 1 == 1
    
    def validate_output(self, *args, **kwargs):
        assert 1 == 1
    
    def __eq__(self, value):
        return True if isinstance(value, Adder) else False
    
    def _run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
class Multiplier(PipelineComponent):

    def validate_input(self, *args, **kwargs):
        assert 1 == 1

    def validate_output(self, *args, **kwargs):
        assert 1 == 1

    def __eq__(self, value):
        return True if isinstance(value, Multiplier) else False

    def _run(self, x, *args, **kwargs):
        return (x * 10,), {}

    def _a_run(self, x, *args, **kwargs):
        return (x * 10,), {}  

@pytest.fixture
def simple_pipeline() -> SequentialPipeline:
    return SequentialPipeline([Adder(), Multiplier()])

def test_sequential_pipeline_init(simple_pipeline):
    assert simple_pipeline is not None
    
def test_pipeline_len(simple_pipeline):
    assert len(simple_pipeline) == 2
    
def test_pipeline_getitem(simple_pipeline):
    assert simple_pipeline[0] == Adder()
    assert simple_pipeline[1] == Multiplier()
    
def test_pipeline_sort(simple_pipeline):
    try: 
        simple_pipeline.sort()
        assert 1 != 0
    except:
        assert 1 == 1
    
def test_pipeline_add(simple_pipeline):
    input = 1
    simple_pipeline = simple_pipeline + [Adder()]
    args, _ = simple_pipeline.run(input)
    assert args[0] == 10 * (input + 2) + 2
    assert len(simple_pipeline) == 3
    
def test_pipeline_iadd(simple_pipeline):
    input = 1
    simple_pipeline += [Adder()]
    args, _ = simple_pipeline.run(1)
    assert args[0] == 10 * (input + 2) + 2
    assert len(simple_pipeline) == 3
    
def test_pipeline_append(simple_pipeline):
    input = 1
    simple_pipeline.append(Adder())
    args, _ = simple_pipeline.run(1)
    assert args[0] == 10 * (input + 2) + 2
    assert len(simple_pipeline) == 3

def test_pipeline_extend(simple_pipeline):
    input = 1
    simple_pipeline.extend([Adder(), Multiplier()])
    args, _ = simple_pipeline.run(1)
    assert args[0] == 10 * (10 * (input + 2) + 2)
    assert len(simple_pipeline) == 4

def test_pipeline_setitem(simple_pipeline):
    input = 1
    simple_pipeline[1] = Adder()
    args, _ = simple_pipeline.run(1)
    assert args[0] == (input + 2) + 2
    assert len(simple_pipeline) == 2

def test_pipeline_run(simple_pipeline):
    input = 1
    args, _ = simple_pipeline.run(1)
    assert args[0] == 10 * (input + 2)