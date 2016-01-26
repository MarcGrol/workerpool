package workerpool

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type WorkerFunc func(interface{})

type task struct {
	f    WorkerFunc
	data interface{}
}

type worker struct {
	quit  chan bool
	tasks chan *task
}

func createWorker(pool *WorkerPool) *worker {
	worker := &worker{
		quit:  make(chan bool),
		tasks: make(chan *task),
	}

	// start processing tasks in background
	go worker.processTasks(pool)

	return worker
}

func (w *worker) processTasks(pool *WorkerPool) {
	// mark worker as started
	pool.wg.Done()

	// process tasks untill data received in quit channel
outerloop:
	for {
		select {
		case <-w.quit:
			log.Printf("Worker got termination signal")
			break outerloop

		case task := <-w.tasks:
			task.f(task.data)

			// return current worker to the pool of available workers
			pool.availableWorkers.addTail(w)

		}
	}

	// cleanup
	close(w.tasks)
	close(w.quit)

	// mark worker as stopped
	pool.wg.Done()
	log.Printf("Worker terminated")

}

func sendTaskToWorker(w *worker, t *task) {
	w.tasks <- t
}

func terminateWorker(w *worker) {
	w.quit <- true
}

type WorkerPool struct {
	wg               sync.WaitGroup
	mutex            sync.Mutex
	up               bool
	workers          []*worker
	availableWorkers *workerQueue
}

func NewWorkerPool(size int) *WorkerPool {
	pool := WorkerPool{
		workers:          make([]*worker, size),
		availableWorkers: &workerQueue{},
	}

	return &pool
}

func (pool *WorkerPool) Start() {
	if pool.isUp() == false {

		pool.availableWorkers.open(len(pool.workers))

		for i := 0; i < len(pool.workers); i++ {
			// needed to be able to determine startup completion
			pool.wg.Add(1)

			// create worker
			pool.workers[i] = createWorker(pool)

			// make worker available for performing task
			pool.availableWorkers.addTail(pool.workers[i])
		}

		// wait untill all workers have started
		pool.wg.Wait()

		// mark as up
		pool.setUp(true)
	}
}

func (pool *WorkerPool) Stop() {
	if pool.isUp() == true {

		// accept no more new work
		pool.setUp(false)

		// terminate all workers
		for i := 0; i < len(pool.workers); i++ {
			pool.wg.Add(1)
			terminateWorker(pool.workers[i])
		}

		// wait untill all workers have terminated
		pool.wg.Wait()

		// cleanup
		pool.availableWorkers.close()

		log.Printf("Pool terminated")
	}
}

func (pool *WorkerPool) Execute(f WorkerFunc, data interface{}, timeoutSecs int) error {
	// only accept work when pool is up
	if pool.isUp() == false {
		return fmt.Errorf("Pool is not running")
	}

	// Get a worker:  operation could block
	worker, err := pool.availableWorkers.getHead(time.Duration(timeoutSecs) * time.Second)
	if err != nil {
		return err
	}

	// send work to the available background worker
	sendTaskToWorker(worker, &task{f: f, data: data})

	return nil
}

func (pool *WorkerPool) setUp(isUp bool) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.up = isUp
}

func (pool *WorkerPool) isUp() bool {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	return pool.up
}

type workerQueue struct {
	pipe chan *worker
}

func (queue *workerQueue) open(size int) {
	// need buffered channel to allow for multiple writes without a read on the other side
	queue.pipe = make(chan *worker, size)
}

func (queue *workerQueue) close() {
	close(queue.pipe)
}

func (q *workerQueue) addTail(worker *worker) {
	// use channel as queue
	q.pipe <- worker
}

func (q *workerQueue) getHead(timeout time.Duration) (*worker, error) {
	timer := time.After(timeout)
	for {
		select {
		case <-timer:
			return nil, fmt.Errorf("Timeout waiting for worker")

		case worker := <-q.pipe:
			return worker, nil
		}
	}
}
