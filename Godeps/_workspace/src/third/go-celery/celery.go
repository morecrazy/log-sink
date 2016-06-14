package celery

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

type Deliveries chan *Task

type Worker interface {
	Exec(*Task) (interface{}, error)
}

var (
	RetryError  = errors.New("Retry task again")
	RejectError = errors.New("Reject task")
)

var draining bool = false
var wg sync.WaitGroup

type Celery struct {
	worker_num_percpu int
	deliveries        chan *Task
	broker            *Broker
	queue             *Queue
	broker_info       string
	queue_info        string
	registry          map[string]Worker
}

func NewCelery(broker_str, queue_str string, cache_len, worker_num_percpu int) *Celery {
	var celery = Celery{
		broker_info:       broker_str,
		queue_info:        queue_str,
		worker_num_percpu: worker_num_percpu,
	}
	celery.deliveries = make(Deliveries, cache_len)
	celery.registry = make(map[string]Worker)
	return &celery
}

func (celery *Celery) RegisterTask(name string, worker Worker) {
	celery.registry[name] = worker
}

func shutdown(status int) {
	fmt.Println("\nceleryd: Warm shutdown")
	os.Exit(status)
}

func (celery *Celery) Init() {

	runtime.GOMAXPROCS(runtime.NumCPU())
	celery.broker = NewBroker(celery.broker_info, celery)

	for key, _ := range celery.registry {
		fmt.Printf("  %s\n", key)
	}

	celery.queue = NewDurableQueue(celery.queue_info)

	celery.broker.conn.DeclareQueue(celery.queue)
}

func (celery *Celery) Publish(p *Publishing) error {
	p.Key = celery.queue.Name
	return celery.broker.Publish(p)
}

func (celery *Celery) StartConsuming() {

	//执行优雅退出
	go ShouldShutDown()
	//启动固定数量的worker，避免worker过多 把队列中的任务都刷下来却又执行不了
	for i := 0; i < celery.worker_num_percpu*runtime.NumCPU(); i++ {
		go celery.Work()
	}
	//开始从队列中获取消息
	celery.broker.StartConsuming(celery.queue, runtime.NumCPU())
}

func ShouldShutDown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	for _ = range c {
		// If interrupting for the second time,
		// terminate un-gracefully
		if draining {
			shutdown(1)
		}
		fmt.Println("\nceleryd: Hitting Ctrl+C again will terminate all running tasks!")
		// Gracefully shut down
		draining = true
		go func() {
			wg.Wait()
			shutdown(0)
		}()
	}
}

func (celery *Celery) Work() {
	for !draining {
		task := <-celery.deliveries
		celery._Work(task)
	}
}

func (celery *Celery) _Work(task *Task) {
	wg.Add(1)

	defer wg.Done()
	if worker, ok := celery.registry[task.Task]; ok {
		logger.Info("Got task from broker: %s", task)
		start := time.Now()
		result, err := worker.Exec(task)
		end := time.Now()
		if err != nil {
			switch err {
			case RetryError:
				task.Requeue()
			default:
				task.Reject()
			}
		} else {
			res, _ := json.Marshal(result)
			str := fmt.Sprintf("Task %s succeeded in %s: %s", task, end.Sub(start), res)
			logger.Notice(str)
			task.Ack(result)
		}
	} else {
		task.Reject()
		logger.Error("Received unregistered task of type '%s'.\nThe message has been ignored and discarded.\n", task.Task)
	}
}
