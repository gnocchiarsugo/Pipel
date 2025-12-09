import pytest
from pipel import SequentialPipeline, PipelineComponent, UnsafePipelineComponent, PipelData


"""
    Advanced because we set a way to know when pipes are equal.
    This changes the outcomes of some list methods
"""


class Adder(UnsafePipelineComponent):
    
    def __eq__(self, value):
        if not isinstance(value, Adder):
            return False
        else:
            return True if self.id == value.id else False
    
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
    
class Multiplier(UnsafePipelineComponent):

    def __eq__(self, value):
        if not isinstance(value, Multiplier):
            return False
        else:
            return True if self.id == value.id else False

    def _run(self, x):
        return PipelData(
            args = (x * 10,),
            kwargs = {}
        )

    def _a_run(self, x):
        return PipelData(
            args = (x * 10,),
            kwargs = {}
        )

@pytest.fixture
def simple_pipeline() -> SequentialPipeline:
    return SequentialPipeline([Adder(), Multiplier()])
    
def test_pipeline_index(simple_pipeline):
    first_el = simple_pipeline[0]
    assert simple_pipeline.index(first_el) == 0
    last_el = Multiplier()
    simple_pipeline.append(last_el)
    assert simple_pipeline.index(last_el) == 2

def test_pipeline_count(simple_pipeline):
    # Since the pipes are all unique becouse of ids
    assert simple_pipeline.count(simple_pipeline[0]) == 1
    assert simple_pipeline.count(Multiplier()) == 0
    assert simple_pipeline.count(Adder()) == 0

def test_pipeline_remove(simple_pipeline):
    with pytest.raises(expected_exception=ValueError):
        simple_pipeline.remove(Adder())
        ## Adder, given the equality criteria, is not in list
    assert len(simple_pipeline) == 2
    simple_pipeline.remove(simple_pipeline[0])
    assert len(simple_pipeline) == 1
    
def test_pipeline_contains(simple_pipeline):
    assert Adder() not in simple_pipeline
    assert simple_pipeline[0] in simple_pipeline
    