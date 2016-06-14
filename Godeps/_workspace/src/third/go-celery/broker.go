package celery

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	TwoSeconds          = 2 * time.Second
	MaximumRetriesError = errors.New("Maximum retries exceeded")
)

type Broker struct {
	conn   *Connection
	celery *Celery
}

func (b *Broker) StartConsuming(q *Queue, rate int) {

	for {
		messages, err := b.conn.Consume(q, rate)
		if err != nil {
			logger.Error(err.Error())
			time.Sleep(TwoSeconds)
			continue
		}
		for msg := range messages {
			task := &Task{
				Receipt: msg.Receipt,
			}
			switch msg.ContentType {
			case "application/json":
				json.Unmarshal(msg.Body, &task)
			default:
				logger.Warning("Unsupported content-type [%s]", msg.ContentType)
				// msg.Reject(false)
				return
			}
			b.celery.deliveries <- task
		}
	}

}

func (b *Broker) Publish(p *Publishing) error {
	return b.conn.Publish(p)
}

func NewBroker(uri string, celery *Celery) *Broker {
	var scheme = strings.SplitN(uri, "://", 2)[0]

	if transport, ok := transportRegistry[scheme]; ok {
		driver := transport.Open(uri)
		conn := NewConnection(driver)
		broker := Broker{conn, celery}
		broker.celery = celery
		return &broker
	}

	panic(fmt.Sprintf("Unknown transport [%s]", scheme))
}
