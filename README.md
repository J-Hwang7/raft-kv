# raft-kv
Created by Diego Ongaro and John Ousterhout, the [Raft Consensus Algorithm](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14) is a leader-based consensus algorithm that manages replicated logs across servers. In Go, I built a key-value store using the Raft consensus algorithm by creating three nodes that elect a leader, replicates client logs, and keeps serving when a node dies.

# How it works
Similar to an election, 

# Licenses
Ongaro, D., & Ousterhout, J. (2014). In Search of an Understandable Consensus Algorithm. 
