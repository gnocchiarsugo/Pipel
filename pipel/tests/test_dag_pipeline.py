import pytest
from pipel import UnsafePipelineComponent, PipelData, DAGPipeline

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
        
class Multiplier(UnsafePipelineComponent):
    
    def _run(self, x, y):
        return PipelData(
            args=(x * y,)
        )
        
    def _a_run(self, x,y):
        return PipelData(
            args=(x * y,)
        )         
        
def test_dag_pipeline_init():
    dag_pipeline = DAGPipeline(
        [Adder(), Adder()],
        adj = [
            [0, 1],
            [0, 0]
        ]
    )
    assert dag_pipeline is not None

def test_dag_pipeline_onestate_not_dag():
    with pytest.raises(expected_exception=ValueError):
        DAGPipeline(
            [Adder()],
            adj = [[1]]
        )
    
def test_dag_pipeline_twostates_not_dag():
    with pytest.raises(expected_exception=ValueError):
        DAGPipeline(
            [Adder(), Adder()],
            adj = [
                [0, 1],
                [1, 0]
            ]
        )
    
def test_dag_pipeline_more_inputs_than_start():
    input_data1 = PipelData(args=(1,))
    input_data2 = PipelData(args=(2,))
    dag = DAGPipeline(
        [Adder()],
        adj = [[0]]
    )
    with pytest.raises(expected_exception=AssertionError):
        dag.run({0: input_data1, 1:input_data2})
        
def test_dag_pipeline_less_inputs_than_start():
    input_data = PipelData(args=(1,))
    dag = DAGPipeline(
        [Adder(), Adder()],
        adj = [
            [0,0],
            [0,0],
        ]
    )
    with pytest.raises(expected_exception=AssertionError, match="The number of starting states is different than the number of inuputs given."):
        dag.run({0: input_data})
    
def test_dag_pipeline_data_to_non_starting():
    input_data = PipelData(args=(1,))
    dag = DAGPipeline(
        [Adder(), Adder(), Adder()],
        adj = [
            [0,1,0],
            [0,0,0],
            [0,0,0],
        ]
    )
    # The input len is the same as the number of starting but the indicies are wrong
    with pytest.raises(expected_exception=AssertionError, match="All states given input must be starting states."):
        dag.run({0:input_data, 1:input_data})
 
def test_dag_pipeline_non_dict():
    dag = DAGPipeline(
        [Adder(), Adder(), Adder()],
        adj = [
            [0,1,0],
            [0,0,0],
            [0,0,0],
        ]
    )
    with pytest.raises(expected_exception=AssertionError, match="Input data container must be a dictionary."):
        dag.run(1)
    
def test_dag_pipeline_non_pipeldata():
    dag = DAGPipeline(
        [Adder(), Adder(), Adder()],
        adj = [
            [0,1,0],
            [0,0,0],
            [0,0,0],
        ]
    )
    with pytest.raises(expected_exception=AssertionError, match="The input data must be of type PipelData."):
        dag.run({0:1})
    
# Functionality

def test_dag_pipeline_onestate():
    input_data = PipelData(args=(10,))
    dag = DAGPipeline(
        [Adder()],
        adj = [[0]]
    )
    res = dag.run({0: input_data})
    assert res[0].args[0] == input_data.args[0] + 2
    
def test_dag_pipeline_simple_sequential():
    input_data = PipelData(args=(10,))
    dag = DAGPipeline(
        [Adder(), Adder()],
        adj = [[0,1],
               [0,0]
            ]
    )
    res = dag.run({0: input_data})
    assert res[1].args[0] == input_data.args[0] + 4
  
def test_dag_pipeline_simple_sequential_intermediate_result():
    input_data = PipelData(args=(10,))
    dag = DAGPipeline(
        [Adder(), Adder()],
        adj = [[0,1],
               [0,0]
            ]
    )
    res = dag.run({0: input_data})
    assert res[1].args[0] == input_data.args[0] + 4
    
def test_dag_pipeline_args_merge_input_data():
    input_data = PipelData(args=(10,))
    dag = DAGPipeline(
        [Adder(), Adder(), Multiplier()],
        adj = [
            [0,0,1],
            [0,0,1],
            [0,0,0]
        ]
    )
    res = dag.run({0:input_data, 1:input_data})
    assert res[2].args[0] == 12**2
    
def test_dag_pipeline_default_kwargs_merge_input_data():
    """If kwargs merges the most recent node data is the one given"""
    class kAdder(UnsafePipelineComponent):
        def _run(self, *, x:int=0):
            return PipelData(
                args=(),
                kwargs={'x': x + 2}
            )
        def _a_run(self, *, x:int=0):
            return PipelData(
                args=(),
                kwargs={'x': x + 2}
            )
    input_data1 = PipelData(args=(), kwargs={'x':1})
    input_data2 = PipelData(args=(), kwargs={'x':2}) # This one is the one that passes
    dag = DAGPipeline(
        [kAdder(), kAdder(), kAdder()],
            adj = [
                [0,0,1],
                [0,0,1],
                [0,0,0],
            ]
    )
    res = dag.run({0:input_data1, 1:input_data2})
    assert res[2].kwargs['x'] == 2 + 4

def test_dag_pipeline_custom_kwargs_merge_input_data():
    """Can customize how kwargs merge globally"""
    class kAdder(UnsafePipelineComponent):
        def _run(self, *, x:int=0):
            return PipelData(
                args=(),
                kwargs={'x': x + 2}
            )
        def _a_run(self, *, x:int=0):
            return PipelData(
                args=(),
                kwargs={'x': x + 2}
            )
            
    def custom_kwargs_merge(kwargs1, kwargs2):
        for k, v in kwargs2.items():
            kwargs1[k] = kwargs1.get('x', 0) + v
        return kwargs1
            
    input_data1 = PipelData(args=(), kwargs={'x':1})
    input_data2 = PipelData(args=(), kwargs={'x':2}) # This one is the one that passes
    dag = DAGPipeline(
        [kAdder(), kAdder(), kAdder()],
            adj = [
                [0,0,1],
                [0,0,1],
                [0,0,0],
            ]
    )
    res = dag.run({0:input_data1, 1:input_data2}, kwargs_merge_func=custom_kwargs_merge)
    assert res[2].kwargs['x'] == ((1 + 2) + (2 + 2)) + 2

def test_dag_pipeline_branching():
    """The output of one node can be broadcasted to multiple other nodes"""
    input_data1 = PipelData(args=(1,))
    dag = DAGPipeline(
        [Adder(), Adder(), Adder()],
            adj = [
                [0,1,1],
                [0,0,0],
                [0,0,0],
            ]
    )
    res = dag.run({0: input_data1})
    assert res[1].args[0] == 1+2+2
    assert res[2].args[0] == 1+2+2
    
def test_dag_pipeline_correct_data_propag():
    """If one path has more nodes than another, if both connect to one node, the program needs to wait for all inputs to be present"""
    input_data = PipelData(args=(1,))
    dag = DAGPipeline(
        [Adder(), Adder(), Adder(), Multiplier()],
            adj = [
                [0,1,0,0],
                [0,0,0,1],
                [0,0,0,1],
                [0,0,0,0],
            ]
    )
    res = dag.run({0:input_data, 2:input_data})
    assert res[3].args[0] == (input_data.args[0] + 4) * (input_data.args[0] + 2)
    
