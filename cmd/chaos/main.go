// Command chaos is a minimal failure injector (spec §6.4): after a delay it
// SIGKILLs a target process (a worker), simulating a mid-stream crash so the
// recovery path can be measured. The Phase 5 exit test drives it.
package main

import (
	"flag"
	"log"
	"syscall"
	"time"
)

func main() {
	var (
		pid   = flag.Int("pid", 0, "target process id to kill")
		after = flag.Duration("after", 3*time.Second, "delay before killing")
	)
	flag.Parse()
	if *pid == 0 {
		log.Fatal("chaos: --pid is required")
	}
	log.Printf("chaos: will SIGKILL pid %d after %s", *pid, *after)
	time.Sleep(*after)
	if err := syscall.Kill(*pid, syscall.SIGKILL); err != nil {
		log.Fatalf("chaos: kill %d: %v", *pid, err)
	}
	log.Printf("chaos: killed pid %d at %d", *pid, time.Now().UnixMilli())
}
