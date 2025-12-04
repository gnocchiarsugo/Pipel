import time
from multiprocessing import Queue
from pipel.multiprocessing import PicklablePipelineComponent, PipelPool

class Adder(PicklablePipelineComponent):
    
    def _run(self, x, *args, **kwargs):
        time.sleep(0.5)
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
class LongAdder(PicklablePipelineComponent):
    
    def _run(self, x, *args, **kwargs):
        time.sleep(3)
        return (x + 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x + 2,), {}
    
class Multiplier(PicklablePipelineComponent):
    
    def _run(self, x, *args, **kwargs):
        time.sleep(0.5)
        return (x * 2,), {}
    
    def _a_run(self, x, *args, **kwargs):
        return (x * 2,), {}


def test_pool_init():
    adder_pool = PipelPool(Adder())
    assert adder_pool is not None
    adder_pool.close()
    
def test_pool_close():
    adder_pool = PipelPool(Adder())
    adder_pool.put(10)
    adder_pool.close()
    ## Assert worker list is empty
    assert not adder_pool.workers
    assert len(adder_pool) == 0
    
def test_pool_long_close():
    """Wait for long operations to terminate and then terminate"""
    adder_pool = PipelPool(LongAdder())
    adder_pool.put(10)
    adder_pool.close()
    ## Assert worker list is empty
    assert not adder_pool.workers
    assert len(adder_pool) == 0
    
def test_pool_long_close_idempotency():
    adder_pool = PipelPool(LongAdder())
    adder_pool.close()
    adder_pool.close()
    
def test_adder_pool_put():
    adder_pool = PipelPool(Adder())
    input_data:int = 10
    adder_pool.put(input_data)
    res, _ = adder_pool.get()
    assert res[0] == input_data + 2
    adder_pool.close()

def test_pool_out_queue():
    """If the out queue is specified, it stores the worker outputs"""
    out_queue = Queue()
    adder_pool = PipelPool(Adder(), out_queue=out_queue)
    input_data:int = 10
    adder_pool.put(input_data)
    res, _ = out_queue.get()
    assert res[0] == input_data + 2
    adder_pool.close()
    
    # External queues manual management
    out_queue.close()
    out_queue.join_thread()
    
def test_pool_context_manager():
    input_data:int = 10
    with PipelPool(Adder()) as pool:
        pool.put(input_data)
        res, _ = pool.get()
    assert res[0] == 12
    
def test_pool_graceful_termination():
    """When terminating a process, first wait for the result"""
    out_queue = Queue()
    with PipelPool(Adder(), out_queue=out_queue) as pool:
        pool.put(10)
    res, _ = out_queue.get()
    assert res[0] == 12
    
    # External queues manual management
    out_queue.close()
    out_queue.join_thread()
    
def test_pool_refresh_same():
    """We can change the component and the queues in medias res the call refresh"""
    """Refresh with the same out_queue"""
    sub_queue = Queue()
    with PipelPool(Adder(), out_queue=sub_queue) as pool:
        pool.component = Multiplier()
        pool.refresh(num_workers=2, out_queue=sub_queue)
        pool.put(10)
        assert len(pool) == 2
    res, _ = sub_queue.get()
    assert res[0] == 20
    
    # External queues manual management
    sub_queue.close()
    sub_queue.join_thread()
        
def test_pool_refresh_different():
    """We can change the component and the queues in medias res the call refresh"""
    """Refresh with the same out_queue"""
    sub_queue1 = Queue()
    sub_queue2 = Queue()
    
    with PipelPool(Adder(), out_queue=sub_queue1) as pool:
        pool.component = Multiplier()
        pool.refresh(num_workers=2, out_queue=sub_queue2)
        pool.put(10)
        assert len(pool) == 2
    res, _ = sub_queue2.get()
    assert res[0] == 20
    
    # External queues manual management
    sub_queue1.close()
    sub_queue1.join_thread()
    sub_queue2.close()
    sub_queue2.join_thread()
    
        
def test_pool_add_worker():
    num_workers = 2
    with PipelPool(Adder(), num_workers=num_workers) as pool:
        pool.add_workers(2)
        pool.put(10)
        assert len(pool) == 2 + 2

    