import time
from multiprocessing import Queue
from pipel.multiprocessing import ManagedPipeline, PipelPool, PicklablePipelineComponent

class Adder(PicklablePipelineComponent):
    def _run(self, x):
        return (x + 2,), {}

class LongAdder(PicklablePipelineComponent):
    def _run(self, x):
        time.sleep(2)
        return (x + 2,), {}

class Multiplier(PicklablePipelineComponent):
    def _run(self, x):
        return (x * 10,), {}

def test_managed_pipeline():
    """Basic Pipeline functionality"""
    managed_pipeline = ManagedPipeline([
        PipelPool(Adder(), num_workers=1),
        PipelPool(Multiplier(), num_workers=1),
    ])
    managed_pipeline.put(10)
    res, _ = managed_pipeline.get()
    managed_pipeline.close()
    assert res[0] == 120
    
def test_managed_pipeline_context_manager():
    """ManagedPipeline supports the context manager protocol"""
    input_data:int = 2
    with ManagedPipeline([
        PipelPool(Adder()),
        PipelPool(Multiplier())
    ]) as pool:
        pool.put(input_data)
        res, _ = pool.get()
    assert res[0] == 10 * (input_data + 2)
    
def test_managed_pipeline_input_output():
    """The Queues given at init can be used to transmit and obtain data"""
    in_queue = Queue()
    out_queue = Queue()
    input_data:int = 100
    with ManagedPipeline([
        PipelPool(Multiplier()),
        PipelPool(Adder()),
        PipelPool(Multiplier()),
    ], in_queue=in_queue, out_queue=out_queue) as pool:
        # pool.put(input_data)
        in_queue.put(((input_data, ), {}))
        res, _ = out_queue.get()
    assert res[0] == 10 * (10 * input_data + 2)
    
    # External queues manual management
    in_queue.close()
    in_queue.join_thread()
    out_queue.close()
    out_queue.join_thread()
    
    
def test_managed_pipeline_refresh():
    in_queue = Queue()
    out_queue = Queue()
    input_data:int = 100
    with ManagedPipeline([PipelPool(Multiplier()), PipelPool(Adder()), PipelPool(Multiplier())]) as pool:
        pool.refresh_pipes(in_queue=in_queue, out_queue=out_queue)
        in_queue.put(((input_data, ), {}))
        res, _ = out_queue.get()
    assert res[0] == 10 * (10 * input_data + 2)
    
    # External queues manual management
    in_queue.close()
    in_queue.join_thread()
    out_queue.close()
    out_queue.join_thread()
    
def test_managed_pipeline_close_idempotency():
    managed_pipeline = ManagedPipeline([
        PipelPool(Adder(), num_workers=1),
        PipelPool(Multiplier(), num_workers=1),
    ])
    managed_pipeline.close()
    managed_pipeline.close()
    
    
def test_managed_pipeline_autoscaling():
    scaleup = '[q > 2 for q in qsize]'
    scaledown = '[q < 1 for q in qsize]'
    with ManagedPipeline([
        PipelPool(LongAdder()),
    ]) as pool:
        pool.start_autoscaling(
            scaleup_cond=scaleup,
            scaledown_cond=scaledown,
            update_every=0.2
        )
        for i in range(10):
            pool.put(i)
        
        # Wait for double time so that the autoscaler has time to do its job
        time.sleep(0.4)
        assert len(pool.pipe_pools[0]) > 1
        
def test_managed_pipeline_autoscaling_scaledown_bottomline():
    """Scaledown does scales until there is only one process running"""
    scaleup = '[q > 200 for q in qsize]'
    
    # This is always true
    scaledown = '[q <= 10 for q in qsize]'
    with ManagedPipeline([
        PipelPool(LongAdder()),
    ]) as pool:
        pool.start_autoscaling(
            scaleup_cond=scaleup,
            scaledown_cond=scaledown,
            update_every=0.2
        )
        for i in range(5):
            pool.put(i)
        
        # Wait for double time so that the autoscaler has time to do its job
        time.sleep(0.4)
        assert len(pool.pipe_pools[0]) == 1

def test_managed_pipeline_autoscaling_close():
    scaleup = '[q > 2 for q in qsize]'
    scaledown = '[q < 1 for q in qsize]'
    with ManagedPipeline([
        PipelPool(LongAdder()),
    ]) as pool:
        pool.start_autoscaling(
            scaleup_cond=scaleup,
            scaledown_cond=scaledown,
            update_every=0.1
        )
        for i in range(10):
            pool.put(i)
        
        # Wait for double time so that the autoscaler has time to do its job
        time.sleep(0.3)
        pool.close_autoscaling()
        
        # Wait the update time
        time.sleep(0.4)
        assert not pool.is_autoscaling_running()
        print(len(pool.pipe_pools[0]))
        assert len(pool.pipe_pools[0]) > 1        
        
