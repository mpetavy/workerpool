package common

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func primeSum(n int) int {
	sum := 0
	for i := 2; i < n; i++ {
		if isPrime(i) {
			sum += i
		}
	}
	return sum
}

func isPrime(n int) bool {
	if n <= 1 {
		return false
	}
	for i := 2; i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}

func TestWorkerPool(t *testing.T) {
	pool := NewWorkerPool()
	pool.Start()

	for range 2 {
		wg := sync.WaitGroup{}
		for range 1000 {
			wg.Add(1)
			pool.Submit(func() error {
				defer func() {
					wg.Done()
				}()

				primeSum(500000)

				return nil
			})
		}

		wg.Wait()

		time.Sleep(time.Second)

		require.Equal(t, *FlagWorkerPoolWorkersMin, int(pool.targetWorkers))
	}
}
