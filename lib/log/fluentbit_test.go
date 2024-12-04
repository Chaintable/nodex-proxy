package log

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

const (
	testTCPHost = "127.0.0.1:5170"
)

var (
	testObservabilityLogConfig = ObservabilityLogConfig{
		Enable:   true,
		Endpoint: fmt.Sprintf("fluent-bit-tcp://%s?buffer_size=1", testTCPHost),
	}
)

func newTCPServer(t *testing.T) (net.Listener, func() int) {
	listen, err := net.Listen("tcp", testTCPHost)
	if err != nil {
		t.Fatal("listen error", err)
	}
	var read int
	go func() {
		conn, err := listen.Accept()
		if err != nil {
			t.Error("accept error", err)
			return
		}
		data := make([]byte, 65535)
		read, err = conn.Read(data)
		if err != nil {
			t.Error("accept error", err)
			return
		}
		Info(fmt.Sprintf("read data: %s", string(data[:read])))
		if read <= 0 {
			t.Error("no data read")
			return
		}
	}()
	return listen, func() int {
		return read
	}
}

func TestFluentBitSink(t *testing.T) {
	BuildObservabilityLogger(testObservabilityLogConfig, "test", nil)
	for i := 0; i < KB; i++ {
		Observe("test", context.TODO())
	}
	time.Sleep(10 * time.Second)
	Observe("test", context.TODO())
	tcpServer, getRead := newTCPServer(t)
	defer tcpServer.Close()
	time.Sleep(10 * time.Second)
	if getRead() <= 0 {
		t.Error("no data read")
	}
	if getRead() >= MB {
		t.Error("received data size exceed")
	}
}
