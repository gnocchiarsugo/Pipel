from typing import List, Dict, Any
from pipel import PipelData, UnsafePipelineComponent
from collections import deque


"""Directed Acyclic Graph (DAG) Pipeline"""

class DAGPipeline():
    components: List[UnsafePipelineComponent]
    adj: List[List[bool]]
    n: int
    
    def __init__(self, components: List[UnsafePipelineComponent], adj: List[List[bool]]):
        self.components = components
        self.n = len(adj)
        if not self._is_dag(adj):
            raise ValueError("The given adj must be DAG")
        self.adj = adj
    
    def _is_dag(self, adj):
        """
        Validate whether an adjacency matrix represents a DAG.
        adj: list[list[int]] or list[list[bool]]
        Returns True if DAG, False otherwise.
        """
        if any(len(row) != self.n for row in adj):
            raise ValueError("Adjacency matrix must be square")

        for i in range(self.n):
            if adj[i][i] != 0:
                return False  # self-loop = not a DAG

        visited = [0] * self.n   # 0 = unvisited, 1 = in recursion stack, 2 = done

        def dfs(v):
            if visited[v] == 1:
                return True   # cycle found
            if visited[v] == 2:
                return False  # already processed

            visited[v] = 1  # mark as in recursion stack

            for u in range(self.n):
                if adj[v][u] != 0:     # edge v → u
                    if dfs(u):
                        return True
            visited[v] = 2  # finished
            return False

        for v in range(self.n):
            if dfs(v):
                return False

        return True

    def _get_start(self) -> List[int]:
        """In a DAG the starting nodes have 0 columns

        Returns:
            List[int]: indices of the starting nodes
        """
        start_nodes = []
        for col in range(self.n):
            if all([not self.adj[row][col] for row in range(self.n)]):
                start_nodes.append(col)
        return start_nodes
    
    def _get_terminal(self) -> List[int]:
        """In a DAG the ending nodes have 0 rows

        Returns:
            List[int]: indices of the terminal nodes
        """
        terminal_nodes = []
        for row in range(self.n):
            if all([not self.adj[row][col] for col in range(self.n)]):
                terminal_nodes.append(row)
        return terminal_nodes
    
    def _next_state(self, node: int) -> List[int]:
        """Gets the adjacent nodes of node

        Args:
            index (int): Node index

        Returns:
            List[int]: List of adjecent nodes
        """
        return [i for i, flag in enumerate(self.adj[node]) if flag]
        
    def _prev_state(self, node: int) -> List[int]:
        return [i for i in range(self.n) if self.adj[i][node]]
        
    @staticmethod
    def _default_kwargs_merge(kwarg1: Dict[str, Any], kwarg2: Dict[str, Any]) -> Dict[str, Any]:
        kwarg1.update(kwarg2)
        return kwarg1
        
    def run(self, data: Dict[int, PipelData], kwargs_merge_func = _default_kwargs_merge) -> Dict[int, PipelData]:
        """kwargs_merge_func is the custom function that expresses how the kwargs of two PipelData need to merge"""
        
        starting_s = self._get_start()
        assert isinstance(data, dict), "Input data container must be a dictionary."
        assert all(isinstance(d, PipelData) for d in data.values()), "The input data must be of type PipelData."
        assert all(s in starting_s for s in data.keys()), "All states given input must be starting states."
        assert len(starting_s) == len(data.keys()), "The number of starting states is different than the number of inuputs given."

        # Compute in-degrees for all nodes
        in_deg = {node: len(self._prev_state(node)) for node in range(self.n)}
        
        # Storage for merged inputs before processing
        pending_inputs: Dict[int, PipelData] = {}
        
        # Nodes ready to process
        ready_nodes = deque()
        
        for node, d in data.items():
            pending_inputs[node] = d
            ready_nodes.append(node)   # start nodes are ready_nodes by definition

        results = {}  # final processed values
        
        while ready_nodes:
            node = ready_nodes.popleft()
            input_data = pending_inputs.pop(node)

            # Process component for this node
            output_data: PipelData = self.components[node](input_data)
            results[node] = output_data

            # Send data to children, but track readiness
            for child in self._next_state(node):
                if child not in pending_inputs:
                    pending_inputs[child] = PipelData((), {})

                pending_inputs[child].args += output_data.args
                pending_inputs[child].kwargs = kwargs_merge_func(pending_inputs[child].kwargs, output_data.kwargs)
                
                # reduce in-degree
                in_deg[child] -= 1

                # Child becomes ready_nodes only when all parents delivered data
                if in_deg[child] == 0:
                    ready_nodes.append(child)

        # results now contains output of all nodes
        return {k: v for k, v in results.items() if k in self._get_terminal()}

        
        
__all__ = [
    'DAGPipeline'
]
