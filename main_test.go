package main

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func managerFactories() map[string]func() Manager {
	return map[string]func() Manager{
		"mutex": func() Manager { return NewMutexManager(1) },
		"aa":    func() Manager { return NewAAManager(1, 4096) },
		"am":    func() Manager { return NewAManager(1, 64) },
	}
}

func TestManagersDoNotDuplicateRepeatedReverts(t *testing.T) {
	for name, factory := range managerFactories() {
		t.Run(name, func(t *testing.T) {
			manager := factory()
			if got := manager.Allocate(0); got != 0 {
				t.Fatalf("first nonce = %d, want 0", got)
			}
			manager.Revert(0, 0)
			manager.Revert(0, 0)
			if got := manager.Allocate(0); got != 0 {
				t.Fatalf("reused nonce = %d, want 0", got)
			}
			if got := manager.Allocate(0); got != 1 {
				t.Fatalf("nonce after reuse = %d, want 1", got)
			}
		})
	}
}

func TestManagersReuseEveryRevertedNonce(t *testing.T) {
	for name, factory := range managerFactories() {
		t.Run(name, func(t *testing.T) {
			manager := factory()
			const count = 1024
			allocated := make([]uint64, count)
			for i := range allocated {
				allocated[i] = manager.Allocate(0)
			}

			var wg sync.WaitGroup
			for _, nonce := range allocated {
				wg.Add(1)
				go func(n uint64) {
					defer wg.Done()
					manager.Revert(0, n)
				}(nonce)
			}
			wg.Wait()

			seen := make(map[uint64]struct{}, count)
			for range count {
				nonce := manager.Allocate(0)
				if nonce >= count {
					t.Fatalf("returned nonce %d outside reverted range", nonce)
				}
				if _, duplicate := seen[nonce]; duplicate {
					t.Fatalf("returned nonce %d twice", nonce)
				}
				seen[nonce] = struct{}{}
			}
		})
	}
}

func TestManagersReorgStartsWithCleanState(t *testing.T) {
	for name, factory := range managerFactories() {
		t.Run(name, func(t *testing.T) {
			manager := factory()
			for i := 0; i < 10; i++ {
				manager.Allocate(0)
			}
			manager.Revert(0, 8)
			manager.ReorgShift(0, 5)
			for want := uint64(5); want < 10; want++ {
				if got := manager.Allocate(0); got != want {
					t.Fatalf("after reorg got %d, want %d", got, want)
				}
			}
		})
	}
}

func TestManagersConcurrentAllocateAndRevert(t *testing.T) {
	for name, factory := range managerFactories() {
		t.Run(name, func(t *testing.T) {
			manager := factory()
			const workers = 16
			const iterations = 2000
			active := make(map[uint64]struct{})
			var activeMu sync.Mutex
			var wg sync.WaitGroup
			errors := make(chan error, workers)

			for worker := 0; worker < workers; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < iterations; i++ {
						nonce := manager.Allocate(0)
						activeMu.Lock()
						if _, duplicate := active[nonce]; duplicate {
							activeMu.Unlock()
							errors <- fmt.Errorf("nonce %d allocated concurrently", nonce)
							return
						}
						active[nonce] = struct{}{}
						activeMu.Unlock()
						runtime.Gosched()
						activeMu.Lock()
						delete(active, nonce)
						activeMu.Unlock()
						manager.Revert(0, nonce)
					}
				}()
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				t.Error(err)
			}
		})
	}
}

func TestAAReportsCapacityOverflow(t *testing.T) {
	manager := NewAAManager(1, 64)
	manager.Revert(0, 64)
	if got := manager.DroppedReverts(); got != 1 {
		t.Fatalf("dropped reverts = %d, want 1", got)
	}
}

func TestAASummariesDoNotStrandConcurrentReverts(t *testing.T) {
	manager := NewAAManager(1, 4096)
	const workers = 32
	const iterations = 5000
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				nonce := manager.Allocate(0)
				manager.Revert(0, nonce)
			}
		}()
	}
	wg.Wait()

	frontier := manager.states[0].next.Load()
	if frontier > 4096 {
		t.Fatalf("allocation frontier %d exceeded test capacity", frontier)
	}
	seen := make(map[uint64]struct{}, frontier)
	for range frontier {
		nonce := manager.Allocate(0)
		if nonce >= frontier {
			t.Fatalf("nonce %d extended frontier before all %d holes were reused", nonce, frontier)
		}
		if _, duplicate := seen[nonce]; duplicate {
			t.Fatalf("nonce %d returned twice while draining holes", nonce)
		}
		seen[nonce] = struct{}{}
	}
}
