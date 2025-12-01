import pytest
from pipel import SequentialPipeline, PipelineComponent

"""
    Advanced because we set a way to know when pipes are equal.
    This changes the outcomes of some list methods
"""


class Adder(PipelineComponent):
    
    def validate_input(self, *args, **kwargs):
        assert 1 == 1
    
    def validate_output(self, *args, **kwargs):
        assert 1 == 1
    
    def __eq__(self, value):
        if not isinstance(value, Adder):
            return False
        else:
            return True if self.id == value.id else False
    
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
        if not isinstance(value, Multiplier):
            return False
        else:
            return True if self.id == value.id else False

    def _run(self, x, *args, **kwargs):
        return (x * 10,), {}

    def _a_run(self, x, *args, **kwargs):
        return (x * 10,), {}  

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
    try:
        simple_pipeline.remove(Adder())
        ## Adder, given the equality criteria, is not in list
        assert 1 == 0
    except ValueError:
        pass
    assert len(simple_pipeline) == 2
    simple_pipeline.remove(simple_pipeline[0])
    assert len(simple_pipeline) == 1
    
def test_pipeline_contains(simple_pipeline):
    assert Adder() not in simple_pipeline
    assert simple_pipeline[0] in simple_pipeline
    