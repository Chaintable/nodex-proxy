package utils

import (
	"sync"
)

// io.Copy use 32 * 1024 as buffer size too
const bufferPoolSize = 32 * 1024

// bufferPool is a httputil.BufferPool
type bufferPool struct {
	pool sync.Pool
}

func NewBufferPool() *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: newBuffer,
		},
	}
}

func newBuffer() interface{} {
	return make([]byte, bufferPoolSize)
}

func (b *bufferPool) Get() []byte {
	return b.pool.Get().([]byte)
}

func (b *bufferPool) Put(bytes []byte) {
	b.pool.Put(bytes)
}
