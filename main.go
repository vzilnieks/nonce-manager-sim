package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/bits"
	mrand "math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	mode        = flag.String("manager", "mutex", "mutex | aa | am")
	clients     = flag.Int("clients", 500, "number of concurrent clients")
	qps         = flag.Int("qps", 5000, "target requests per second")
	durationSec = flag.Int("secs", 30, "test duration (seconds)")
	faultProb   = flag.Float64("fault", 0.1, "probability of tx failure (revert)")
	addrs       = flag.Int("addrs", 50, "distinct addresses involved")
	reorgAfter  = flag.Int("reorgAfter", -1, "seconds after start to trigger reorg; <0 = none")
	maxDelta    = flag.Int("maxDelta", 20, "max  shift per account at reorg")
	runID       = flag.String("run-id", "default", "")
	warmupSec   = flag.Int("warmup-secs", 5, "warm-up duration excluded from measurements")
	seed        = flag.Int64("seed", 1, "workload seed; 0 chooses a time-based seed")
	resultFile  = flag.String("result-file", "", "append one JSON result to this file")

	// AA uses a fixed-size tree
	capBits = flag.Int("capBits", 1<<18, "AA BitTree capacity in bits")
	// AM grows from an initial window
	amWindow = flag.Int("am-window", 1<<14, "AM window size in bits")

	metricsAddr = flag.String("metrics", ":2112", "Prometheus listen address")
)

type ReserveMetrics struct {
	Latency prometheus.Observer // seconds
}

func NewReserveMetrics(reg prometheus.Registerer, base prometheus.Labels) *ReserveMetrics {
	hv := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nonce",
		Subsystem: "manager",
		Name:      "allocate_latency_seconds",
		Help:      "Latency of Allocate() calls",
		Buckets:   prometheus.ExponentialBuckets(1e-7, 2, 25),
	}, []string{"manager", "fault", "run_id"})
	reg.MustRegister(hv)
	return &ReserveMetrics{Latency: hv.With(base)}
}

type Manager interface {
	Allocate(addr int) uint64
	Revert(addr int, nonce uint64)
	ReorgShift(addr int, newNext uint64)
	CurrentNext(addr int) uint64
}

type BitTree struct {
	l0 []atomic.Uint64
	l1 []atomic.Uint64
	l2 atomic.Uint64
}

const maxBitTreeBits = 64 * 64 * 64

func newBitTree(bitsCap int) *BitTree {
	if bitsCap <= 0 {
		bitsCap = 64
	}
	if bitsCap > maxBitTreeBits {
		bitsCap = maxBitTreeBits
	}
	words0 := (bitsCap + 63) / 64
	l0 := make([]atomic.Uint64, words0)
	words1 := (words0 + 63) / 64
	l1 := make([]atomic.Uint64, words1)
	var bt BitTree
	bt.l0 = l0
	bt.l1 = l1
	bt.l2.Store(0)
	return &bt
}

func setAtomicBit(word *atomic.Uint64, mask uint64) {
	for {
		old := word.Load()
		if old&mask != 0 || word.CompareAndSwap(old, old|mask) {
			return
		}
	}
}

func clearAtomicBit(word *atomic.Uint64, mask uint64) {
	for {
		old := word.Load()
		if old&mask == 0 || word.CompareAndSwap(old, old&^mask) {
			return
		}
	}
}

func (b *BitTree) publishWord(w0 int) {
	w1 := w0 / 64
	if w1 >= len(b.l1) {
		return
	}
	setAtomicBit(&b.l1[w1], uint64(1)<<uint(w0%64))
	setAtomicBit(&b.l2, uint64(1)<<uint(w1))
}

func (b *BitTree) clearWordSummary(w0 int) {
	w1 := w0 / 64
	if w0 >= len(b.l0) || w1 >= len(b.l1) || b.l0[w0].Load() != 0 {
		return
	}
	mask1 := uint64(1) << uint(w0%64)
	clearAtomicBit(&b.l1[w1], mask1)
	if b.l0[w0].Load() != 0 {
		b.publishWord(w0)
		return
	}
	b.clearRootSummary(w1)
}

func (b *BitTree) clearRootSummary(w1 int) {
	if w1 >= len(b.l1) || b.l1[w1].Load() != 0 {
		return
	}
	mask2 := uint64(1) << uint(w1)
	clearAtomicBit(&b.l2, mask2)
	if b.l1[w1].Load() != 0 {
		setAtomicBit(&b.l2, mask2)
	}
}

func (b *BitTree) Set(i uint64) bool {
	w0 := int(i / 64)
	b0 := uint(i % 64)
	if w0 >= len(b.l0) {
		return false
	}
	mask0 := uint64(1) << b0
	setAtomicBit(&b.l0[w0], mask0)
	b.publishWord(w0)
	return true
}

func (b *BitTree) FindAndClearMin() (uint64, bool) {
	for {
		l2 := b.l2.Load()
		if l2 == 0 {
			return 0, false
		}
		i1 := bits.TrailingZeros64(l2)
		if i1 < 0 || i1 >= 64 {
			return 0, false
		}
		w1 := i1
		if w1 >= len(b.l1) {
			clearAtomicBit(&b.l2, uint64(1)<<uint(w1))
			continue
		}
		l1 := b.l1[w1].Load()
		if l1 == 0 {
			b.clearRootSummary(w1)
			continue
		}
		i0 := bits.TrailingZeros64(l1)
		if i0 < 0 || i0 >= 64 {
			continue
		}
		w0 := w1*64 + i0
		if w0 >= len(b.l0) {
			clearAtomicBit(&b.l1[w1], uint64(1)<<uint(i0))
			b.clearRootSummary(w1)
			continue
		}
		l0 := b.l0[w0].Load()
		if l0 == 0 {
			b.clearWordSummary(w0)
			continue
		}
		bb := bits.TrailingZeros64(l0)
		if bb < 0 || bb >= 64 {
			continue
		}
		mask := uint64(1) << uint(bb)
		if b.l0[w0].CompareAndSwap(l0, l0&^mask) {
			if (l0 &^ mask) == 0 {
				b.clearWordSummary(w0)
			}
			idx := uint64(w0)*64 + uint64(bb)
			return idx, true
		}
		// Another allocator won CAS. Start again from the current root.
	}
}

func (b *BitTree) Clear() {
	for i := range b.l0 {
		b.l0[i].Store(0)
	}
	for i := range b.l1 {
		b.l1[i].Store(0)
	}
	b.l2.Store(0)
}

type aaAddrState struct {
	next    atomic.Uint64
	holes   *BitTree
	reorgMu sync.RWMutex
}

type AAManager struct {
	states         []*aaAddrState
	droppedReverts atomic.Uint64
}

func NewAAManager(nAddrs int, capBits int) *AAManager {
	s := make([]*aaAddrState, nAddrs)
	for i := 0; i < nAddrs; i++ {
		s[i] = &aaAddrState{holes: newBitTree(capBits)}
		s[i].next.Store(0)
	}
	return &AAManager{states: s}
}

func (m *AAManager) Allocate(addr int) uint64 {
	st := m.states[addr]
	st.reorgMu.RLock()
	defer st.reorgMu.RUnlock()
	if idx, ok := st.holes.FindAndClearMin(); ok {
		return idx
	}
	return st.next.Add(1) - 1
}

func (m *AAManager) Revert(addr int, nonce uint64) {
	st := m.states[addr]
	st.reorgMu.RLock()
	if !st.holes.Set(nonce) {
		m.droppedReverts.Add(1)
	}
	st.reorgMu.RUnlock()
}

func (m *AAManager) DroppedReverts() uint64 { return m.droppedReverts.Load() }

func (m *AAManager) CurrentNext(addr int) uint64 { return m.states[addr].next.Load() }

func (m *AAManager) ReorgShift(addr int, newNext uint64) {
	st := m.states[addr]
	st.reorgMu.Lock()
	st.holes.Clear()
	st.next.Store(newNext)
	st.reorgMu.Unlock()
}

type amAddrState struct {
	next      atomic.Uint64
	hint      atomic.Uint64 // word index to start scanning from
	holeCount atomic.Int64
	holes     []atomic.Uint64 // L0 words
	reorgMu   sync.RWMutex
}

type AMManager struct {
	states  []*amAddrState
	winBits int // window size and growth step in bits
}

func NewAManager(nAddrs int, winBits int) *AMManager {
	if winBits < 64 {
		winBits = 64
	}
	words := (winBits + 63) / 64
	s := make([]*amAddrState, nAddrs)
	for i := 0; i < nAddrs; i++ {
		h := make([]atomic.Uint64, words)
		s[i] = &amAddrState{holes: h}
		s[i].next.Store(0)
		s[i].hint.Store(0)
	}
	return &AMManager{states: s, winBits: winBits}
}

func (m *AMManager) Allocate(addr int) uint64 {
	st := m.states[addr]
	st.reorgMu.RLock()
	defer st.reorgMu.RUnlock()

	if st.holeCount.Load() == 0 {
		return st.next.Add(1) - 1
	}

	words := len(st.holes)
	if words == 0 {
		return st.next.Add(1) - 1
	}
	// There cannot be a reusable nonce after next-1
	nxt := st.next.Load()
	if nxt == 0 {
		return st.next.Add(1) - 1
	}
	hi := int((nxt - 1) / 64)
	if hi >= words {
		hi = words - 1
	}
	start := int(st.hint.Load())
	if start < 0 || start > hi {
		start = 0
	}

	scan := func(from, to int) (uint64, bool) {
		for i := from; i <= to; i++ {
			w := st.holes[i].Load()
			if w == 0 {
				continue
			}
			for w != 0 {
				b := bits.TrailingZeros64(w)
				if b < 0 || b >= 64 {
					break
				}
				mask := uint64(1) << uint(b)
				old := st.holes[i].Load()
				if old&mask == 0 {
					w = st.holes[i].Load()
					continue
				}
				if st.holes[i].CompareAndSwap(old, old&^mask) {
					st.holeCount.Add(-1)
					// Keep the next scan close to the last useful word
					if i >= start {
						st.hint.Store(uint64(i))
					}
					return uint64(i)*64 + uint64(b), true
				}
				w = st.holes[i].Load()
			}
		}
		return 0, false
	}

	if n, ok := scan(start, hi); ok {
		return n
	}
	if start > 0 {
		if n, ok := scan(0, start-1); ok {
			return n
		}
	}
	return st.next.Add(1) - 1
}

func (m *AMManager) Revert(addr int, nonce uint64) {
	st := m.states[addr]
	need := int(nonce) + 1

	st.reorgMu.RLock()
	if need <= len(st.holes)*64 {
		st.setHole(nonce)
		st.reorgMu.RUnlock()
		return
	}
	st.reorgMu.RUnlock()

	// Recheck after acquiring the exclusive lock
	st.reorgMu.Lock()
	if need > len(st.holes)*64 {
		target := roundUp(need, m.winBits)
		newWords := (target + 63) / 64
		newH := make([]atomic.Uint64, newWords)
		for i := range st.holes {
			newH[i].Store(st.holes[i].Load())
		}
		st.holes = newH
	}
	st.setHole(nonce)
	st.reorgMu.Unlock()
}

func (st *amAddrState) setHole(nonce uint64) {
	word := int(nonce / 64)
	bit := uint(nonce % 64)
	mask := uint64(1) << bit
	for {
		old := st.holes[word].Load()
		if old&mask != 0 {
			break
		}
		if st.holes[word].CompareAndSwap(old, old|mask) {
			st.holeCount.Add(1)
			break
		}
	}
	widx := uint64(word)
	for {
		h := st.hint.Load()
		if widx >= h || st.hint.CompareAndSwap(h, widx) {
			break
		}
	}
}

func (m *AMManager) ReorgShift(addr int, newNext uint64) {
	st := m.states[addr]
	st.reorgMu.Lock()
	for i := range st.holes {
		st.holes[i].Store(0)
	}
	st.holeCount.Store(0)
	st.next.Store(newNext)
	newHint := int(newNext / 64)
	if newHint < 0 {
		newHint = 0
	}
	if newHint >= len(st.holes) {
		newHint = len(st.holes) - 1
	}
	if newHint < 0 {
		newHint = 0
	}
	st.hint.Store(uint64(newHint))
	st.reorgMu.Unlock()
}

func (m *AMManager) CurrentNext(addr int) uint64 { return m.states[addr].next.Load() }

// MutexManager
type u64Heap []uint64

func heapPush(h *u64Heap, v uint64) {
	*h = append(*h, v)
	i := len(*h) - 1
	for i > 0 {
		p := (i - 1) / 2
		if (*h)[p] <= (*h)[i] {
			break
		}
		(*h)[p], (*h)[i] = (*h)[i], (*h)[p]
		i = p
	}
}
func heapPopMin(h *u64Heap) (uint64, bool) {
	if len(*h) == 0 {
		return 0, false
	}
	min := (*h)[0]
	last := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	if len(*h) > 0 {
		(*h)[0] = last
		i := 0
		for {
			l := 2*i + 1
			r := 2*i + 2
			if l >= len(*h) {
				break
			}
			m := l
			if r < len(*h) && (*h)[r] < (*h)[l] {
				m = r
			}
			if (*h)[i] <= (*h)[m] {
				break
			}
			(*h)[i], (*h)[m] = (*h)[m], (*h)[i]
			i = m
		}
	}
	return min, true
}

type mutexAddrState struct {
	mu    sync.Mutex
	next  uint64
	holes u64Heap
	seen  map[uint64]struct{}
}

type MutexManager struct{ states []*mutexAddrState }

func NewMutexManager(nAddrs int) *MutexManager {
	s := make([]*mutexAddrState, nAddrs)
	for i := 0; i < nAddrs; i++ {
		s[i] = &mutexAddrState{seen: make(map[uint64]struct{})}
	}
	return &MutexManager{states: s}
}

func (m *MutexManager) Allocate(addr int) uint64 {
	st := m.states[addr]
	st.mu.Lock()
	defer st.mu.Unlock()
	if v, ok := heapPopMin(&st.holes); ok {
		delete(st.seen, v)
		return v
	}
	v := st.next
	st.next++
	return v
}

func (m *MutexManager) Revert(addr int, nonce uint64) {
	st := m.states[addr]
	st.mu.Lock()
	if _, ok := st.seen[nonce]; !ok {
		st.seen[nonce] = struct{}{}
		heapPush(&st.holes, nonce)
	}
	st.mu.Unlock()
}

func (m *MutexManager) ReorgShift(addr int, newNext uint64) {
	st := m.states[addr]
	st.mu.Lock()
	st.next = newNext
	st.holes = nil
	clear(st.seen)
	st.mu.Unlock()
}

func (m *MutexManager) CurrentNext(addr int) uint64 {
	st := m.states[addr]
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.next
}

type workItem struct {
	addr         int
	shouldRevert bool
	measured     bool
	phase        *sync.WaitGroup
}

type runResult struct {
	SchemaVersion  int     `json:"schema_version"`
	Timestamp      string  `json:"timestamp"`
	RunID          string  `json:"run_id"`
	Manager        string  `json:"manager"`
	Seed           int64   `json:"seed"`
	GOMAXPROCS     int     `json:"gomaxprocs"`
	Clients        int     `json:"clients"`
	QPS            int     `json:"qps"`
	Duration       int     `json:"duration_seconds"`
	Warmup         int     `json:"warmup_seconds"`
	Fault          float64 `json:"fault"`
	Addresses      int     `json:"addrs"`
	Reorg          bool    `json:"reorg"`
	ReorgAfter     int     `json:"reorg_after_seconds"`
	MaxDelta       int     `json:"max_delta"`
	CapBits        int     `json:"cap_bits"`
	AMWindow       int     `json:"am_window"`
	WarmupOps      uint64  `json:"warmup_operations"`
	OfferedOps     uint64  `json:"offered_operations"`
	Operations     uint64  `json:"operations"`
	Reverts        uint64  `json:"reverts"`
	DroppedWork    uint64  `json:"dropped_work"`
	DroppedReverts uint64  `json:"dropped_reverts"`
	AchievedQPS    float64 `json:"achieved_qps"`
	Avg            float64 `json:"avg"`
	P50            float64 `json:"p50"`
	P95            float64 `json:"p95"`
	P99            float64 `json:"p99"`
	P999           float64 `json:"p999"`
	Max            float64 `json:"max"`
	OperationAvg   float64 `json:"operation_avg"`
	OperationP50   float64 `json:"operation_p50"`
	OperationP95   float64 `json:"operation_p95"`
	OperationP99   float64 `json:"operation_p99"`
	OperationP999  float64 `json:"operation_p999"`
	OperationMax   float64 `json:"operation_max"`
	RevertAvg      float64 `json:"revert_avg"`
	RevertP50      float64 `json:"revert_p50"`
	RevertP95      float64 `json:"revert_p95"`
	RevertP99      float64 `json:"revert_p99"`
	RevertP999     float64 `json:"revert_p999"`
	RevertMax      float64 `json:"revert_max"`
	Valid          bool    `json:"valid"`
	InvalidReason  string  `json:"invalid_reason,omitempty"`
}

func main() {
	flag.Parse()
	validateFlags()

	workloadSeed := *seed
	if workloadSeed == 0 {
		workloadSeed = time.Now().UnixNano()
	}

	mgr := makeManager(*mode, *addrs, *capBits, *amWindow)
	mx := startMetrics(*metricsAddr, *mode, *faultProb, *runID)

	nWorkers := *clients
	if nWorkers <= 0 {
		nWorkers = runtime.GOMAXPROCS(0)
	}
	workCh := make(chan workItem, nWorkers*2)
	allocateSamples := make([][]int64, nWorkers)
	operationSamples := make([][]int64, nWorkers)
	revertSamples := make([][]int64, nWorkers)
	ready := sync.WaitGroup{}
	workers := sync.WaitGroup{}
	ready.Add(nWorkers)
	workers.Add(nWorkers)
	targetOperations := *qps * *durationSec
	perWorkerCapacity := targetOperations/nWorkers + 16
	for i := 0; i < nWorkers; i++ {
		go func(workerID int) {
			defer workers.Done()
			allocateSamples[workerID] = make([]int64, 0, perWorkerCapacity)
			operationSamples[workerID] = make([]int64, 0, perWorkerCapacity)
			revertSamples[workerID] = make([]int64, 0, perWorkerCapacity/4+1)
			ready.Done()
			for item := range workCh {
				if item.measured {
					started := time.Now()
					nonce := mgr.Allocate(item.addr)
					allocateElapsed := time.Since(started)
					operationElapsed := allocateElapsed
					if item.shouldRevert {
						revertStarted := time.Now()
						mgr.Revert(item.addr, nonce)
						revertElapsed := time.Since(revertStarted)
						operationElapsed += revertElapsed
						revertSamples[workerID] = append(revertSamples[workerID], int64(revertElapsed))
					}
					allocateSamples[workerID] = append(allocateSamples[workerID], int64(allocateElapsed))
					operationSamples[workerID] = append(operationSamples[workerID], int64(operationElapsed))
					if mx != nil {
						mx.Latency.Observe(allocateElapsed.Seconds())
					}
				} else {
					nonce := mgr.Allocate(item.addr)
					if item.shouldRevert {
						mgr.Revert(item.addr, nonce)
					}
				}
				item.phase.Done()
			}
		}(i)
	}
	ready.Wait()

	log.Printf("starting: mode=%s clients=%d qps=%d warmup=%ds secs=%d addrs=%d fault=%.3f seed=%d run_id=%s",
		*mode, nWorkers, *qps, *warmupSec, *durationSec, *addrs, *faultProb, workloadSeed, *runID)

	rng := mrand.New(mrand.NewSource(workloadSeed))
	warmupSubmitted, _ := runLoadPhase(time.Duration(*warmupSec)*time.Second, false, workCh, rng)

	measureCtx, cancelMeasure := context.WithCancel(context.Background())
	var reorgWG sync.WaitGroup
	var reorgTriggered atomic.Bool
	if *reorgAfter >= 0 {
		reorgWG.Add(1)
		go func() {
			defer reorgWG.Done()
			timer := time.NewTimer(time.Duration(*reorgAfter) * time.Second)
			defer timer.Stop()
			select {
			case <-measureCtx.Done():
				return
			case <-timer.C:
			}
			reorgTriggered.Store(true)
			reorgRNG := mrand.New(mrand.NewSource(workloadSeed ^ 0x5deece66d))
			for addr := 0; addr < *addrs; addr++ {
				mgr.ReorgShift(addr, sampleNewNext(mgr, addr, *maxDelta, reorgRNG))
			}
		}()
	}

	submitted, dropped := runLoadPhase(time.Duration(*durationSec)*time.Second, true, workCh, rng)
	cancelMeasure()
	reorgWG.Wait()
	close(workCh)
	workers.Wait()

	latencies := flattenSamples(allocateSamples)
	operationLatencies := flattenSamples(operationSamples)
	revertLatencies := flattenSamples(revertSamples)
	droppedReverts := uint64(0)
	if reporter, ok := mgr.(interface{ DroppedReverts() uint64 }); ok {
		droppedReverts = reporter.DroppedReverts()
	}
	result := summarizeRun(latencies, workloadSeed, nWorkers, warmupSubmitted, submitted, dropped,
		operationLatencies, revertLatencies, droppedReverts, reorgTriggered.Load())
	if err := emitResult(result, *resultFile); err != nil {
		log.Fatalf("write result: %v", err)
	}
	if !result.Valid {
		log.Fatalf("invalid benchmark result: %s", result.InvalidReason)
	}
	log.Printf("finished run_id=%s operations=%d achieved_qps=%.1f p99=%.0fns valid=%t",
		*runID, result.Operations, result.AchievedQPS, result.P99*1e9, result.Valid)
}

func validateFlags() {
	if *addrs <= 0 || *qps <= 0 || *durationSec <= 0 || *warmupSec < 0 {
		log.Fatal("addrs, qps, and secs must be positive; warmup-secs cannot be negative")
	}
	if *faultProb < 0 || *faultProb > 1 {
		log.Fatal("fault must be between 0 and 1")
	}
	if *maxDelta < 0 {
		log.Fatal("maxDelta cannot be negative")
	}
	if *mode == "aa" && (*capBits <= 0 || *capBits > maxBitTreeBits) {
		log.Fatalf("capBits for AA must be between 1 and %d", maxBitTreeBits)
	}
}

func makeManager(name string, nAddrs, aaCapacity, window int) Manager {
	switch name {
	case "aa":
		return NewAAManager(nAddrs, aaCapacity)
	case "am":
		return NewAManager(nAddrs, window)
	case "mutex":
		return NewMutexManager(nAddrs)
	default:
		log.Fatalf("unknown manager mode: %q", name)
		return nil
	}
}

func startMetrics(addr, manager string, fault float64, id string) *ReserveMetrics {
	if addr == "" {
		return nil
	}
	reg := prometheus.NewRegistry()
	mx := NewReserveMetrics(reg, prometheus.Labels{
		"manager": manager,
		"fault":   fmt.Sprintf("%.2f", fault),
		"run_id":  id,
	})
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintln(w, "ok") })
	go func() {
		log.Printf("serving metrics at %s/metrics", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	return mx
}

func runLoadPhase(duration time.Duration, measured bool, workCh chan<- workItem, rng *mrand.Rand) (uint64, uint64) {
	if duration <= 0 {
		return 0, 0
	}
	interval := time.Second / time.Duration(*qps)
	if interval <= 0 {
		interval = time.Microsecond
	}
	ticker := time.NewTicker(interval)
	timer := time.NewTimer(duration)
	defer ticker.Stop()
	defer timer.Stop()

	var phase sync.WaitGroup
	var submitted, dropped uint64
	for {
		select {
		case <-timer.C:
			phase.Wait()
			return submitted, dropped
		case <-ticker.C:
			item := workItem{
				addr:         rng.Intn(*addrs),
				shouldRevert: rng.Float64() < *faultProb,
				measured:     measured,
				phase:        &phase,
			}
			phase.Add(1)
			select {
			case workCh <- item:
				submitted++
			default:
				phase.Done()
				dropped++
			}
		}
	}
}

func flattenSamples(byWorker [][]int64) []int64 {
	total := 0
	for _, workerSamples := range byWorker {
		total += len(workerSamples)
	}
	result := make([]int64, 0, total)
	for _, workerSamples := range byWorker {
		result = append(result, workerSamples...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func summarizeRun(latencies []int64, workloadSeed int64, nWorkers int, warmupOps, submitted, dropped uint64,
	operationLatencies, revertLatencies []int64, droppedReverts uint64, reorgTriggered bool) runResult {
	result := runResult{
		SchemaVersion:  1,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		RunID:          *runID,
		Manager:        *mode,
		Seed:           workloadSeed,
		GOMAXPROCS:     runtime.GOMAXPROCS(0),
		Clients:        nWorkers,
		QPS:            *qps,
		Duration:       *durationSec,
		Warmup:         *warmupSec,
		Fault:          *faultProb,
		Addresses:      *addrs,
		Reorg:          reorgTriggered,
		ReorgAfter:     *reorgAfter,
		MaxDelta:       *maxDelta,
		CapBits:        *capBits,
		AMWindow:       *amWindow,
		WarmupOps:      warmupOps,
		OfferedOps:     submitted + dropped,
		Operations:     uint64(len(latencies)),
		Reverts:        uint64(len(revertLatencies)),
		DroppedWork:    dropped,
		DroppedReverts: droppedReverts,
		AchievedQPS:    float64(len(latencies)) / float64(*durationSec),
		Valid:          droppedReverts == 0 && uint64(len(latencies)) == submitted,
	}
	if droppedReverts != 0 {
		result.InvalidReason = "AA capacity was exceeded; reverted nonces were discarded"
	} else if uint64(len(latencies)) != submitted {
		result.InvalidReason = "not every submitted operation completed"
	}
	if len(latencies) == 0 {
		return result
	}
	var sum int64
	for _, latency := range latencies {
		sum += latency
	}
	result.Avg = time.Duration(sum / int64(len(latencies))).Seconds()
	result.P50 = percentile(latencies, 0.50)
	result.P95 = percentile(latencies, 0.95)
	result.P99 = percentile(latencies, 0.99)
	result.P999 = percentile(latencies, 0.999)
	result.Max = time.Duration(latencies[len(latencies)-1]).Seconds()
	result.OperationAvg, result.OperationP50, result.OperationP95, result.OperationP99,
		result.OperationP999, result.OperationMax = latencySummary(operationLatencies)
	result.RevertAvg, result.RevertP50, result.RevertP95, result.RevertP99,
		result.RevertP999, result.RevertMax = latencySummary(revertLatencies)
	return result
}

func latencySummary(latencies []int64) (float64, float64, float64, float64, float64, float64) {
	if len(latencies) == 0 {
		return 0, 0, 0, 0, 0, 0
	}
	var sum int64
	for _, latency := range latencies {
		sum += latency
	}
	return time.Duration(sum / int64(len(latencies))).Seconds(),
		percentile(latencies, 0.50),
		percentile(latencies, 0.95),
		percentile(latencies, 0.99),
		percentile(latencies, 0.999),
		time.Duration(latencies[len(latencies)-1]).Seconds()
}

func percentile(sorted []int64, quantile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * quantile)
	return time.Duration(sorted[index]).Seconds()
}

func emitResult(result runResult, path string) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(append(data, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func sampleNewNext(mgr Manager, addr, maxDelta int, rng *mrand.Rand) uint64 {
	n := mgr.CurrentNext(addr)
	d := rng.Intn(2*maxDelta+1) - maxDelta
	nn := int64(n) + int64(d)
	if nn < 0 {
		nn = 0
	}
	return uint64(nn)
}

func roundUp(x, step int) int {
	if step <= 0 {
		return x
	}
	return ((x + step - 1) / step) * step
}
