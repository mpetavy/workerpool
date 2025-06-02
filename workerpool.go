package common

import (
	"fmt"
	"github.com/mpetavy/common"
	"github.com/shirou/gopsutil/v4/cpu"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	FlagNameWorkerPoolCPULimit          = "workerpool.cpu.limit"
	FlagNameWorkerPoolJobLimit          = "workerpool.job.limit"
	FlagNameWorkerPoolWorkersMin        = "workerpool.workers.min"
	FlagNameWorkerPoolWorkersMax        = "workerpool.workers.max"
	FlagNameWorkerPoolControllerTimeout = "workerpool.controller.timeout"
)

var (
	FlagWorkerPoolCPULimit          = common.SystemFlagInt(FlagNameWorkerPoolCPULimit, 80, "CPU limit for worker goroutines")
	FlagWorkerPoolJobLimit          = common.SystemFlagInt(FlagNameWorkerPoolJobLimit, 10000, "Jobs limit in queue")
	FlagWorkerPoolWorkersMin        = common.SystemFlagInt(FlagNameWorkerPoolWorkersMin, 1, "Minimum amount of concurrent workers")
	FlagWorkerPoolWorkersMax        = common.SystemFlagInt(FlagNameWorkerPoolWorkersMax, runtime.NumCPU()*2, "Maximum amount of concurrent workers")
	FlagWorkerPoolControllerTimeout = common.SystemFlagInt(FlagNameWorkerPoolControllerTimeout, 1000, "Timeout for the controller goroutine to adjust the number of worker goroutines")
)

type WorkerJob func() error

var (
	currentUsage atomic.Int64
)

type WorkerPool struct {
	mutex          sync.Mutex
	targetUsage    float64
	minimumWorkers float64
	currentWorkers float64
	targetWorkers  float64
	jobQueue       chan WorkerJob
	quit           chan struct{}
}

func init() {
	go func() {
		for {
			usage := int64(GetCPUUsage())

			currentUsage.Store(usage)

			fmt.Printf("%d%%\n", usage)
		}
	}()
}

func NewWorkerPool() *WorkerPool {
	wp := &WorkerPool{
		mutex:          sync.Mutex{},
		targetUsage:    float64(*FlagWorkerPoolCPULimit),
		minimumWorkers: float64(*FlagWorkerPoolWorkersMin),
		currentWorkers: float64(*FlagWorkerPoolWorkersMin),
		targetWorkers:  0,
		jobQueue:       make(chan WorkerJob, *FlagWorkerPoolJobLimit),
		quit:           make(chan struct{}),
	}

	return wp
}

func (workerPool *WorkerPool) Start() {
	workerPool.adjust()

	go workerPool.controller()
}

func (workerPool *WorkerPool) adjust() {
	workerPool.mutex.Lock()
	defer workerPool.mutex.Unlock()

	usage := currentUsage.Load()

	workerPool.adjustTarget(float64(usage))
	workerPool.adjustWorkers()
}

func (workerPool *WorkerPool) Submit(job WorkerJob) {
	workerPool.jobQueue <- job
}

func (workerPool *WorkerPool) Stop() {
	close(workerPool.quit)
	close(workerPool.jobQueue)
}

func (workerPool *WorkerPool) controller() {
	ticker := time.NewTicker(common.MillisecondToDuration(*FlagWorkerPoolControllerTimeout))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			workerPool.adjust()
		case <-workerPool.quit:
			return
		}
	}
}

func (workerPool *WorkerPool) adjustTarget(usage float64) {
	if len(workerPool.jobQueue) == 0 {
		workerPool.targetWorkers = float64(*FlagWorkerPoolWorkersMin)
		return
	}

	if usage > float64(*FlagWorkerPoolCPULimit) {
		// Reduce workers proportionally to overshoot
		workerPool.targetWorkers = workerPool.targetWorkers * float64(*FlagWorkerPoolCPULimit) / usage
	} else {
		// Increase workers by 10% (with upper bound)
		workerPool.targetWorkers = workerPool.targetWorkers + 2
	}

	// Ensure minimum of 1 worker
	workerPool.targetWorkers = common.Min(float64(*FlagWorkerPoolWorkersMax), workerPool.targetWorkers)
	workerPool.targetWorkers = common.Max(float64(*FlagWorkerPoolWorkersMin), workerPool.targetWorkers)
}

func (workerPool *WorkerPool) adjustWorkers() {
	for workerPool.currentWorkers < workerPool.targetWorkers {
		workerPool.currentWorkers++

		go workerPool.worker()
	}
}

func (workerPool *WorkerPool) worker() {
	defer func() {
		workerPool.mutex.Lock()
		workerPool.currentWorkers--
		workerPool.mutex.Unlock()
	}()

	for {
		select {
		case job, ok := <-workerPool.jobQueue:
			if !ok {
				return
			}
			job()

			workerPool.mutex.Lock()
			shouldExit := workerPool.currentWorkers > workerPool.targetWorkers
			workerPool.mutex.Unlock()

			if shouldExit {
				return
			}
		case <-workerPool.quit:
			return
		}
	}
}

func (workerPool *WorkerPool) Debug() {
	common.Debug("-------------------------------------\n")
	common.Debug("CPU usage:       %d\n", currentUsage.Load())
	common.Debug("Current jobs:    %d\n", len(workerPool.jobQueue))
	common.Debug("Current workers: %f\n", workerPool.currentWorkers)
	common.Debug("Target workers:  %f\n", workerPool.targetWorkers)
}

func GetCPUUsage() float64 {
	percent, _ := cpu.Percent(time.Second, false)
	if len(percent) > 0 {
		return percent[0]
	}

	return 0
}
