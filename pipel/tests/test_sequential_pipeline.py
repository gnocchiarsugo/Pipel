import pytest
from pipel import SequentialPipeline, UnsafePipelineComponent, PipelData

class Adder(UnsafePipelineComponent):
    
    def __eq__(self, value):
        return True if isinstance(value, Adder) else False
    
    def _run(self, x, **kwargs):
        return PipelData(
            args = (x + 2,),
            kwargs = {}
        )
    
    def _a_run(self, x, **kwargs):
        return PipelData(
            args = (x + 2,),
            kwargs = {}
        )
    
class Multiplier(UnsafePipelineComponent):

    def __eq__(self, value):
        return True if isinstance(value, Multiplier) else False

    def _run(self, x, **kwargs):
        return PipelData(
            args = (x * 10,),
            kwargs = {}
        )

    def _a_run(self, x, **kwargs):
        return PipelData(
            args = (x * 10,),
            kwargs = {}
        )

@pytest.fixture
def simple_pipeline() -> SequentialPipeline:
    return SequentialPipeline([Adder(), Multiplier()])

def test_sequential_pipeline_init(simple_pipeline):
    assert simple_pipeline is not None
    
def test_sequential_pipeline_copy(simple_pipeline):
    simple_copy = simple_pipeline.copy()
    assert simple_pipeline == simple_copy
    
def test_pipeline_append(simple_pipeline):
    input_data = PipelData(args=(1,))
    simple_pipeline.append(Adder())
    out_data: PipelData = simple_pipeline.run(input_data)
    assert out_data.args[0] == 10 * (input_data.args[0] + 2) + 2
    assert len(simple_pipeline) == 3
    
def test_pipeline_extend(simple_pipeline):
    input_data = PipelData(args=(1,))
    simple_pipeline.extend([Adder(), Multiplier()])
    out_data: PipelData = simple_pipeline.run(input_data)
    assert out_data.args[0] == 10 * (10 * (input_data.args[0] + 2) + 2)
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
    with pytest.raises(expected_exception=NotImplementedError):
        simple_pipeline.sort()
        
def test_pipeline_len(simple_pipeline):
    assert len(simple_pipeline) == 2
    
def test_pipeline_getitem(simple_pipeline):
    assert simple_pipeline[0] == Adder()
    assert simple_pipeline[1] == Multiplier()
    
def test_pipeline_setitem(simple_pipeline):
    input_data = PipelData(args=(1,))
    simple_pipeline[1] = Adder()
    out_data: PipelData = simple_pipeline.run(input_data)
    assert out_data.args[0] == (input_data.args[0] + 2) + 2
    assert len(simple_pipeline) == 2
    
def test_pipeline_add(simple_pipeline):
    input_data = PipelData(args=(1,))
    simple_pipeline = simple_pipeline + [Adder()]
    out_data: PipelData = simple_pipeline.run(input_data)
    assert out_data.args[0] == 10 * (input_data.args[0] + 2) + 2
    assert len(simple_pipeline) == 3
    
def test_pipeline_iadd(simple_pipeline):
    input_data = PipelData(args=(1,))
    simple_pipeline += [Adder()]
    out_data: PipelData = simple_pipeline.run(input_data)
    assert out_data.args[0] == 10 * (input_data.args[0] + 2) + 2
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
    input_data = PipelData(args=(1,))
    out_data:PipelData = simple_pipeline.run(input_data)
    assert out_data.args[0] == 10 * (input_data.args[0] + 2)