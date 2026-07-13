package jsonrpc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/types"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/assert"
)

// Regression: the mirror goroutine must only see snapshots taken on the
// handler goroutine. Hertz recycles the RequestContext (and the request body
// buffer) for the next request as soon as the handler returns; reading them
// asynchronously raced and could leak another request's data to the mirror.
// Run with -race.
func TestRequestMirrorSnapshotsRequestData(t *testing.T) {
	type captured struct {
		method string
		header string
		body   string
	}
	got := make(chan captured, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-release // hold until the original context has been "reused"
		body, _ := io.ReadAll(req.Body)
		got <- captured{req.Method, req.Header.Get("X-Test"), string(body)}
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	assert.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	assert.NoError(t, err)

	mirrorMap := NewMirrorMap()
	mirrorMap.AddMirrorTarget("1", "a", &discovery.MirrorTarget{Address: u.Hostname(), Port: port})

	processor := requestMirrorHertz(5*time.Second, types.RequestMirrorConfig{Enable: true}, mirrorMap, NewMirrorLimiter())

	c := &app.RequestContext{}
	c.Request.SetMethod("POST")
	c.Request.Header.Set("X-Test", "original")
	body := []byte(`{"id":1}`)
	processData := &types.RequestContext{ChainId: "1", RawRequestBody: body}

	processor(context.Background(), c, processData)

	// Simulate hertz recycling the context and buffers for the next request
	// while the mirror call is still in flight.
	c.Request.SetMethod("GET")
	c.Request.Header.Set("X-Test", "poisoned")
	for i := range body {
		body[i] = 'x'
	}
	close(release)

	select {
	case res := <-got:
		assert.Equal(t, "POST", res.method)
		assert.Equal(t, "original", res.header)
		assert.Equal(t, `{"id":1}`, res.body)
	case <-time.After(5 * time.Second):
		t.Fatal("mirror request never reached the test server")
	}
}
