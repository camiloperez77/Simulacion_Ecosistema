from collections import defaultdict, deque
from math import gcd

class MarkovChainAnalysis:
    def __init__(self, transition_matrix):
        """
        Initialize the MarkovChainAnalysis with a transition matrix.

        Parameters:
        transition_matrix (list of list of float): Square matrix representing the transition probabilities
                                                   between states in the Markov chain.
        """
        self.P = transition_matrix
        self.num_states = len(self.P)
        self.adj_list = self.build_adjacency_list()
        self.visited = [False] * self.num_states
        self.low_link = [-1] * self.num_states
        self.ids = [-1] * self.num_states
        self.on_stack = [False] * self.num_states
        self.stack = []
        self.id = 0
        self.scc_components = []

    def build_adjacency_list(self):
        """
        Construct an adjacency list representation of the Markov chain from the transition matrix.
        Only positive transition probabilities are considered as edges.

        Returns:
        defaultdict(list): Adjacency list representation of the graph.
        """
        adj_list = defaultdict(list)
        for i in range(self.num_states):
            for j in range(self.num_states):
                if self.P[i][j] > 0:
                    adj_list[i].append(j)
        return adj_list

    def dfs(self, at):
        """
        Perform a depth-first search (DFS) to identify strongly connected components (SCCs).
        This method uses Tarjan's algorithm to find SCCs.

        Parameters:
        at (int): The index of the current state being visited in the DFS.
        """
        self.visited[at] = True
        self.ids[at] = self.low_link[at] = self.id
        self.id += 1
        self.stack.append(at)
        self.on_stack[at] = True

        # Explore the neighbors
        for to in self.adj_list[at]:
            if not self.visited[to]:
                self.dfs(to)
                self.low_link[at] = min(self.low_link[at], self.low_link[to])
            elif self.on_stack[to]:
                self.low_link[at] = min(self.low_link[at], self.ids[to])

        # Check if we're at the root of an SCC
        if self.ids[at] == self.low_link[at]:
            scc = []
            while self.stack:
                node = self.stack.pop()
                self.on_stack[node] = False
                scc.append(node)
                self.low_link[node] = self.ids[at]
                if node == at:
                    break
            self.scc_components.append(scc)

    def find_sccs(self):
        """
        Find all strongly connected components in the graph using DFS.
        """
        for i in range(self.num_states):
            if not self.visited[i]:
                self.dfs(i)

    def analyze_dtmc(self):
        """
        Analyze the DTMC to classify states into recurrent, transient, periodic, aperiodic, and ergodic.

        Returns:
        dict: A dictionary with sets of state indices for each classification.
        """
        self.find_sccs()
        recurrent_states = {
            state for scc in self.scc_components
            for state in scc
            if len(scc) > 1 or self.P[state][state] > 0
        }
        transient_states = set(range(self.num_states)) - recurrent_states

        periodic_states = set()
        aperiodic_states = set()
        ergodic_states = set()

        for scc in self.scc_components:
            period = self.compute_period(scc[0])
            for state in scc:
                if period > 1:
                    periodic_states.add(state)
                else:
                    aperiodic_states.add(state)
                    if len(scc) > 1 or self.P[state][state] > 0:
                        ergodic_states.add(state)

        return {
            'recurrent_states': recurrent_states,
            'transient_states': transient_states,
            'periodic_states': periodic_states,
            'aperiodic_states': aperiodic_states,
            'ergodic_states': ergodic_states
        }

    def compute_period(self, start_state):
        """
        Compute the period of a state in the Markov chain.

        Parameters:
        start_state (int): The index of the state for which to compute the period.

        Returns:
        int: The period of the state.
        """
        visited = {start_state: 0}
        queue = deque([start_state])
        periods = set()

        while queue:
            state = queue.popleft()
            level = visited[state]

            for next_state in self.adj_list[state]:
                if next_state == start_state:
                    periods.add(level + 1)
                elif next_state not in visited:
                    visited[next_state] = level + 1
                    queue.append(next_state)

        # The period of the state is the greatest common divisor (GCD) of the lengths of all cycles passing through it.
        return self.gcd(periods) if periods else 1

    @staticmethod
    def gcd(numbers):
        """
        Compute the greatest common divisor (GCD) of a set of numbers.

        Parameters:
        numbers (set of int): The set of numbers to compute the GCD for.

        Returns:
        int: The GCD of the numbers.
        """
        x = numbers.pop()
        while numbers:
            y = numbers.pop()
            x = gcd(x, y)
        return x