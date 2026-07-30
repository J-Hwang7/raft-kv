package main

import (
	"fmt"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("goroutine says hi")
	}()
	wg.Wait()
	fmt.Println("raft-kv toolchain works")
}