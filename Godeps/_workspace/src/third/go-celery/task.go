package celery

import (
	"bytes"
	"fmt"
	"third/gouuid"
	"time"
)

const CELERY_FORMAT = `"2006-01-02T15:04:05.999999999"`

type celeryTime struct {
	time.Time
}

var null = []byte("null")

func (ct *celeryTime) UnmarshalJSON(data []byte) (err error) {
	if bytes.Equal(data, null) {
		return
	}
	t, err := time.Parse(CELERY_FORMAT, string(data))
	if err == nil {
		*ct = celeryTime{t}
	}
	return
}

func (ct *celeryTime) MarshalJSON() (data []byte, err error) {
	if ct.IsZero() {
		return null, nil
	}
	return []byte(ct.Format(CELERY_FORMAT)), nil
}

type Receipt interface {
	Reply(string, interface{})
	Ack()
	Requeue()
	Reject()
}

type Task struct {
	Task    string                 `json:"task"`
	Id      string                 `json:"id"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	Eta     celeryTime             `json:"eta"`
	Expires celeryTime             `json:"expires"`
	Receipt Receipt                `json:"-"`
}

func (t *Task) Ack(result interface{}) {
	if result != nil {
		t.Receipt.Reply(t.Id, result)
	}
	t.Receipt.Ack()
}

func (t *Task) Requeue() {
	go func() {
		time.Sleep(time.Second)
		t.Receipt.Requeue()
	}()
}

func (t *Task) Reject() {
	t.Receipt.Reject()
}

func (t *Task) String() string {
	return fmt.Sprintf("%s[%s]", t.Task, t.Id)
}

// Returns a pointer to a new task object
func NewTask(task string, args []interface{}, kwargs map[string]interface{}) (*Task, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	t := Task{
		Task:   task,
		Id:     id.String(),
		Args:   args,
		Kwargs: kwargs,
	}

	return &t, nil
}
