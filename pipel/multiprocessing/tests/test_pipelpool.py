import time
import multiprocessing as mp
from multiprocessing import Queue
from pipel.multiprocessing import PicklablePipelineComponent, PipelPool
from pipel import PipelData

class Adder(PicklablePipelineComponent):
    
    def _run(self, x):
        time.sleep(0.5)
        return PipelData(args=(x + 2,), kwargs={})
    
class LongAdder(PicklablePipelineComponent):
    
    def _run(self, x):
        time.sleep(3)
        return PipelData(args=(x + 2,), kwargs={})
    
class Multiplier(PicklablePipelineComponent):
    
    def _run(self, x):
        time.sleep(0.5)
        return PipelData(args=(x * 2,), kwargs={})



def test_pool_manual_init():
    adder_pool = PipelPool(Adder())
    assert adder_pool is not None
    adder_pool.close()
    
def test_pool_context_manager_init():
    with PipelPool(Adder()) as pool:
        assert pool is not None
    
def test_pool_close():
    """Same call when eiting from context  """
    input_data = PipelData(args=(10,), kwargs={})
    adder_pool = PipelPool(Adder())
    adder_pool.put(input_data)
    adder_pool.close()
    ## Assert worker list is empty
    assert not adder_pool.workers
    assert len(adder_pool) == 0
    
def test_pool_long_close():
    """Wait for long operations to terminate and then terminate"""
    input_data = PipelData(args=(10,), kwargs={})
    adder_pool = PipelPool(LongAdder())
    adder_pool.put(input_data)
    data: PipelData = adder_pool.get()
    adder_pool.close()
    ## Assert worker list is empty
    assert not adder_pool.workers
    assert len(adder_pool) == 0
    assert data.args[0] == 12
    
def test_pool_close_idempotency():
    adder_pool = PipelPool(Adder())
    adder_pool.close()
    adder_pool.close()
    
def test_adder_pool_put():
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder()) as adder_pool:
        adder_pool.put(input_data)
        data: PipelData = adder_pool.get()
    assert data.args[0] == input_data.args[0] + 2
    adder_pool.close()

def test_pool_out_queue():
    """
    If the out queue is specified, it stores the worker outputs. 
    And it's not closed even if the pool's closed
    """
    out_queues = [Queue()]
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder(), out_queues=out_queues) as adder_pool:
        adder_pool.put(input_data)
    
    # Get data after pool's closed
    data: PipelData = out_queues[0].get()
    assert data.args[0] == input_data.args[0] + 2
    
    # External queues manual management
    out_queues[0].close()
    out_queues[0].join_thread()

def test_pool_in_queue():
    """If the in queue is specified, it can send data"""
    in_queues = [Queue()]
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder(), in_queues=in_queues) as adder_pool:
        in_queues[0].put(input_data)
        data: PipelData = adder_pool.get()
    assert data.args[0] == input_data.args[0] + 2
    
    # External queues manual management
    in_queues[0].close()
    in_queues[0].join_thread()
    
def test_pool_add_worker():
    with PipelPool(Adder()) as adder_pool:
        assert len(adder_pool) == 1
        adder_pool.add_workers(1)
        assert len(adder_pool) == 2
    
def test_pool_add_zero_worker():
    with PipelPool(Adder()) as adder_pool:
        assert len(adder_pool) == 1
        adder_pool.add_workers(0)
        assert len(adder_pool) == 1
    
def test_pool_remove_worker():
    with PipelPool(Adder()) as adder_pool:
        adder_pool.add_workers(2)
        time.sleep(0.5)
        assert len(adder_pool) == 3
        adder_pool.remove_workers(2)
        time.sleep(0.5)
        assert len(adder_pool) == 1
        
def test_pool_remove_zero_worker():
    with PipelPool(Adder()) as adder_pool:
        adder_pool.add_workers(2)
        time.sleep(0.5)
        assert len(adder_pool) == 3
        adder_pool.remove_workers(0)
        time.sleep(0.5)
        assert len(adder_pool) == 3

def test_pool_event_queue():
    e = Queue()
    with PipelPool(Adder(), event_queue=e) as adder_pool:
        adder_pool.add_workers(2)
        time.sleep(0.5)
        assert len(adder_pool) == 3
        e.put(PipelPool.STOP_TOKEN)
        time.sleep(2)
        # remove_worker removes terminated workers from the list
        # but this doesn't
        assert len(adder_pool) == 3
        
        # But at least one is termianted
        assert any([not p.is_alive() for p in adder_pool.workers])
        
    e.close()
    e.join_thread()
        
def test_pool_change_component():
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder()) as pool:
        pool.change_component(Multiplier())
        pool.put(input_data)
        data: PipelData = pool.get()
    assert data.args[0] == 20

def test_pool_change_component_out_queue():
    """When changing components out queues are left untouched if not specified"""
    # mp.set_start_method("spawn", force=True)
    o = [Queue()]
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder(), out_queues=o) as pool:
        pool.change_component(Multiplier())
        pool.put(input_data)
    data:PipelData = o[0].get()
    assert data.args[0] == 20

def test_pool_change_component_sub_out_queue():
    """When changing components if out queues are specified they are substituted"""
    o = [Queue()]
    sub_o = [Queue()]
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder(), out_queues=o) as pool:
        pool.change_component(Multiplier(), out_queues=sub_o)
        pool.put(input_data)
        time.sleep(1)

    data:PipelData = sub_o[0].get()
    assert data.args[0] == 20
    
    o[0].close()
    o[0].join_thread()
    sub_o[0].close()
    sub_o[0].join_thread()

def test_pool_change_component_in_queue():
    """When changing components in queues are left untouched if not specified"""
    i = [Queue()]
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder(), in_queues=i) as pool:
        pool.change_component(Multiplier())
        i[0].put(input_data)
        data:PipelData = pool.get()
    assert data.args[0] == 20
    
    i[0].close()
    i[0].join_thread()
    
def test_pool_change_component_sub_in_queue():
    """When changing components if out queues are specified they are substituted"""
    i = [Queue()]
    sub_i = [Queue()]
    input_data = PipelData(args=(10,), kwargs={})
    with PipelPool(Adder(), in_queues=i) as pool:
        pool.change_component(Multiplier(), in_queues=sub_i)
        sub_i[0].put(input_data)
        data:PipelData = pool.get()
    assert data.args[0] == 20
    
    i[0].close()
    i[0].join_thread()
    sub_i[0].close()
    sub_i[0].join_thread()
    
def test_pool_change_component_event_queue():
    """When changing components event queues are left untouched if not specified"""
    e = Queue()
    with PipelPool(Adder(), event_queue=e) as pool:
        pool.change_component(Multiplier())
        pool.add_workers(2)
        time.sleep(0.5)
        assert len(pool) == 3
        e.put(PipelPool.STOP_TOKEN)
        time.sleep(2)
        
        assert len(pool) == 3
        assert any([not p.is_alive() for p in pool.workers])
        
def test_pool_change_component_sub_event_queue():
    """When changing components if event queues are specified they are substituted"""
    e = Queue()
    sub_e = Queue()
    with PipelPool(Adder(), event_queue=e) as pool:
        pool.change_component(Multiplier(), event_queue=sub_e)
        pool.add_workers(2)
        time.sleep(0.5)
        assert len(pool) == 3
        sub_e.put(PipelPool.STOP_TOKEN)
        time.sleep(2)
        
        assert len(pool) == 3
        assert any([not p.is_alive() for p in pool.workers])
    
    e.close()
    e.join_thread()
    sub_e.close()
    sub_e.join_thread()    
    