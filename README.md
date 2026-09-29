# raft-kv
Created by Diego Ongaro and John Ousterhout, the [Raft Consensus Algorithm](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14) is a leader-based consensus algorithm that manages replicated logs across servers in a distributed system. In Go, I built a key-value store using the Raft consensus algorithm by creating three nodes that elect a leader, replicate client logs  over TCP, and keep serving when a node dies. All logs are written to disk, ensuring that restarts will not erase committed data.

# How it works
**Leader Election** 

**Log Replication**

**Commiting** 

**Fault Tolerance** 

# Simulating Election
**Requires Go 1.21+ to run.** In 3 individual terminals, run one of the commands.
```
go run . -id 0 -http localhost:9000
go run . -id 1 -http localhost:9001
go run . -id 2 -http localhost:9002
```

After a node wins the election, open a fourth terminal and run the following commands.
```
curl "http://localhost:9000/put?key=x&value=5"
curl "http://localhost:9000/get?key=x"
curl "http://localhost:9000/status"
```
The value 5 is stored within the key "x", as `/status` retrieves information about a node's term, role, commit index, and key-value history.


# Diagram of raft-kv
```mermaid
flowchart TB
    Client["Client (curl)"]

    subgraph Cluster["Raft Cluster"]
        API["HTTP API"]

        subgraph Raft["Raft Nodes - RPC over TCP"]
           subgraph Leader["Leader"]
                N0["Node 0"]
            end
            subgraph Follower["Followers"]
                N1["Node 1"]
                N2["Node 2"]
            end
        end

    State0["raft-state-0.JSON"]
    State1["raft-state-1.JSON"]
    State2["raft-state-2.JSON"]
    end

    Client -->|"GET / PUT / STATUS"| API
    API --> N0
    N0 -->|"Stores"|State0
    N1 -->|"Stores"|State1
    N2 -->|"Stores"|State2

    N0 <-->|"heartbeat + logs"| N1
    N0 <-->|"heartbeat + logs"| N2 

```

## What I learned

# Project Architecture 
```
raft-kv/
main.go          flags all nodes, acts as an entry point, defines node ID
Raft/
client.go        submits requests
election.go      heartbeat loop, elections, commit advance
httpapi.go       HTTP API
raft.go          logs, applies heartbeat loop, node state
rpc.go           RequestVote & AppendEntries
storage.go       fault tolerance 
```

# References
Ongaro, Diego, and John Ousterhout. _In Search of an Understandable Consensus Algorithm_ 19 July 2014.

“Raft.” _Thesecretlivesofdata.Com_, 2026, https://thesecretlivesofdata.com/raft/. 

“Raft Consensus Algorithm.” _Raft.Github.Io_, https://raft.github.io/.
