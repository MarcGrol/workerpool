package workerpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	TIMEOUT_MSEC = 1
)

func TestUnstartedPool(t *testing.T) {
	pool := NewWorkerPool(1)
	err := doitOnce(pool, "40", 0, 1)
	if err.Error() != "Pool is not running" {
		t.Errorf("expected: %s, actual %s", "Pool not running", err.Error())
	}
}

func TestStoppedPool(t *testing.T) {
	pool := NewWorkerPool(1)
	pool.Start()
	pool.Stop()
	err := doitOnce(pool, "41", 0, 1)
	if err.Error() != "Pool is not running" {
		t.Errorf("expected: %s, actual %s", "Pool not running", err.Error())
	}
}

func TestRestartedPool(t *testing.T) {
	pool := NewWorkerPool(2)

	pool.Start()
	err := doitOnce(pool, "42", 0, 1)
	if err != nil {
		t.Errorf("expected: nil, actual %s", err.Error())
	}
	pool.Stop()

	pool.Start()
	err = doitOnce(pool, "43", 0, 1)
	if err != nil {
		t.Errorf("expected: nil, actual %s", err.Error())
	}
	pool.Stop()
}

func TestTimeout(t *testing.T) {
	pool := NewWorkerPool(1)
	pool.Start()
	defer pool.Stop()

	// claim worker
	go pool.Execute(
		func(data interface{}) {
			time.Sleep(time.Duration(2) * time.Second)
		},
		"first",
		1)

	// be sure worker received this task
	time.Sleep(time.Duration(100) * time.Millisecond)

	// block on second task
	err := doitOnce(pool, "second", 3, 1)
	if err == nil || err.Error() != "Timeout waiting for worker" {
		t.Errorf("expected: %s, actual %v", "Timeout waiting for worker", err)
	}
}

func TestBulk(t *testing.T) {
	pool := NewWorkerPool(1)
	pool.Start()
	defer pool.Stop()

	bulk(t, pool, 1000, 1)
}

func doitOnce(pool *WorkerPool, expected string, delay time.Duration, timeoutSecs int) error {
	var wg sync.WaitGroup
	wg.Add(1)

	actual := ""
	err := pool.Execute(
		func(data interface{}) {
			time.Sleep(time.Duration(timeoutSecs) * time.Second)
			actual = data.(string)
			wg.Done()
		},
		expected,
		timeoutSecs)
	if err != nil {
		return err
	}

	wg.Wait()

	if expected != actual {
		return fmt.Errorf("expected: %s, actual %s", expected, actual)
	}

	return nil
}

type Result struct {
	sync.WaitGroup
	sync.Mutex
	messages []string
}

func (r *Result) add(msg string) {
	r.Lock()
	defer r.Unlock()
	r.messages = append(r.messages, msg)
}

func bulk(t *testing.T, pool *WorkerPool, msg_count int, timeout_sec int) {

	r := Result{}

	for i := 0; i < msg_count; i++ {
		r.Add(1)
		payload := fmt.Sprintf("message_%d", i)
		pool.Execute(
			func(data interface{}) {
				r.add(data.(string))
				r.Done()
			},
			payload,
			timeout_sec)
	}

	r.Wait()

	if len(r.messages) != msg_count {
		t.Errorf("expected: %d, actual %d", msg_count, len(r.messages))
	}

	for i := 0; i < msg_count; i++ {
		found := false
		for _, msg := range r.messages {
			if msg == fmt.Sprintf("message_%d", i) {
				found = true
				break
			}
		}
		if found == false {
			t.Errorf("message_%d not found", i)
		}
	}
}
