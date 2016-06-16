package main

import (
	"bytes"
	"sync"
)
type LogBuffer struct {
	m *sync.Mutex
	buf *bytes.Buffer
	len int64
	ch chan bool
	name string
}

func (b *LogBuffer) WriteString(s string) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()
	b.len ++
	if b.len == gLogBufferSize {
		b.ch <- true
	}
	return b.buf.WriteString(s)
}

func (b *LogBuffer) ReadString() string {
	b.m.Lock()
	defer b.m.Unlock()
	str := b.buf.String()
	b.buf.Reset()
	b.len = 0
	return str
}
