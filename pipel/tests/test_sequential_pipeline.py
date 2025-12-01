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
    
def test_sequential_pipeline_copy(simple_pipeline):
    simple_copy = simple_pipeline.copy()
    assert simple_pipeline == simple_copy
    
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

def test_pipeline_pop(simple_pipeline):
    pipe = simple_pipeline[0]
    popped_pipe = simple_pipeline.pop(0)
    assert pipe == popped_pipe
    assert len(simple_pipeline) == 1

## Advanced, in this instance two pipes are equal if they are
# of the same class
def test_pipeline_index(simple_pipeline):
    assert simple_pipeline.index(Adder()) == 0
    assert simple_pipeline.index(Multiplier()) == 1
    simple_pipeline.append(Multiplier())
    assert simple_pipeline.index(Multiplier()) == 1

def test_pipeline_count(simple_pipeline):
    assert simple_pipeline.count(Adder()) == 1
    assert simple_pipeline.count(Multiplier()) == 1
    simple_pipeline.append(Adder())
    assert simple_pipeline.count(Adder()) == 2
    
def test_pipeline_insert(simple_pipeline):
    el = Multiplier
    simple_pipeline.insert(1, el)
    assert simple_pipeline[1] == el

def test_pipeline_remove(simple_pipeline):
    simple_pipeline.remove(Adder())
    assert len(simple_pipeline) == 1
    assert simple_pipeline[0] == Multiplier()

def test_pipeline_sort(simple_pipeline):
    try: 
        simple_pipeline.sort()
        assert 1 != 0
    except:
        assert 1 == 1
        
def test_pipeline_len(simple_pipeline):
    assert len(simple_pipeline) == 2
    
def test_pipeline_getitem(simple_pipeline):
    assert simple_pipeline[0] == Adder()
    assert simple_pipeline[1] == Multiplier()
    
def test_pipeline_setitem(simple_pipeline):
    input = 1
    simple_pipeline[1] = Adder()
    args, _ = simple_pipeline.run(1)
    assert args[0] == (input + 2) + 2
    assert len(simple_pipeline) == 2
    
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
    

def test_pipeline_mul(simple_pipeline):
    pipeline = simple_pipeline * 3
    assert len(pipeline) == len(simple_pipeline) * 3
    assert pipeline[2] == Adder()
    assert pipeline[3] == Multiplier()

def test_pipeline_rmul(simple_pipeline):
    pipeline = 3 * simple_pipeline
    assert len(pipeline) == len(simple_pipeline) * 3
    assert pipeline[2] == Adder()
    assert pipeline[3] == Multiplier()

def test_pipeline_imul(simple_pipeline):
    simple_pipeline *= 3
    assert len(simple_pipeline) == 3 * 2
    assert simple_pipeline[2] == Adder()
    assert simple_pipeline[3] == Multiplier()

def test_pipeline_run(simple_pipeline):
    input = 1
    args, _ = simple_pipeline.run(1)
    assert args[0] == 10 * (input + 2)