package raft

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (rn *RaftNode) StartHTTP(addr string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		idx, isLeader := rn.Submit(Command{Op: "PUT", Key: key, Value: value})
		if !isLeader {
			http.Error(w, "not leader — try another node", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "accepted PUT %s=%s at log index %d\n", key, value, idx)
	})

	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		idx, isLeader := rn.Submit(Command{Op: "DELETE", Key: key})
		if !isLeader {
			http.Error(w, "not leader — try another node", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "accepted DELETE %s at log index %d\n", key, idx)
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val, ok, isLeader := rn.Get(key)
		if !isLeader {
			http.Error(w, "not leader — try another node", http.StatusServiceUnavailable)
			return
		}
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "%s\n", val)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		rn.mu.Lock()
		defer rn.mu.Unlock()
		resp := map[string]any{
			"id":          rn.id,
			"state":       rn.state.String(),
			"term":        rn.currentTerm,
			"commitIndex": rn.commitIndex,
			"lastApplied": rn.lastApplied,
			"logLength":   rn.lastLogIndex(),
			"kv":          rn.kv,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			rn.logf("HTTP server error: %v", err)
		}
	}()
	rn.logf("HTTP API listening on %s", addr)
}