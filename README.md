# raft-kv
Created by Diego Ongaro and John Ousterhout, the [Raft Consensus Algorithm](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14) is a leader-based consensus algorithm that manages replicated logs across servers in a distributed system. In Go, I built a key-value store using the Raft consensus algorithm by creating three nodes that elect a leader, replicate client logs, and keep serving when a node dies. All logs are written to disk, ensuring that restarts will not erase committed data.

# How it works
Similar to a political election, 

# References
Ongaro, Diego, and John Ousterhout. _In Search of an Understandable Consensus Algorithm_ 19 July 2014.

“Raft.” _Thesecretlivesofdata.Com_, 2026, https://thesecretlivesofdata.com/raft/. 
“Raft Consensus Algorithm.” _Raft.Github.Io_, https://raft.github.io/.
