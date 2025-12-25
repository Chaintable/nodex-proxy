package lb

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Chaintable/nodex-proxy/discovery"
	"github.com/Chaintable/nodex-proxy/lb/lbnode"
)

type rpcReq struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

func newNodeForServer(t *testing.T, srv *httptest.Server, source string) *lbnode.Node {
	t.Helper()

	host, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := net.LookupPort("tcp", portStr)
	if err != nil {
		t.Fatalf("LookupPort(%q): %v", portStr, err)
	}

	node, err := lbnode.New("n1", host, port, 1, discovery.NodeTypeState, lbnode.WithSource(source))
	if err != nil {
		t.Fatalf("lbnode.New: %v", err)
	}
	return node
}

func TestProcessHealthCheck_UsesEthBlockNumberForNative(t *testing.T) {
	var gotMethod string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotMethod = req.Method

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer srv.Close()

	hc := NewNodeHealthChecker(2*time.Second, 2*time.Second)
	node := newNodeForServer(t, srv, "native")

	ok := hc.processHealthCheck(node)
	if !ok {
		t.Fatalf("expected health check ok")
	}
	if gotMethod != "eth_blockNumber" {
		t.Fatalf("expected method eth_blockNumber, got %q", gotMethod)
	}
}

func TestProcessHealthCheck_UsesGetLatestBlockForNonNative(t *testing.T) {
	var gotMethod string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var req rpcReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotMethod = req.Method

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{"height":123},"id":1}`))
	}))
	defer srv.Close()

	hc := NewNodeHealthChecker(2*time.Second, 2*time.Second)
	node := newNodeForServer(t, srv, "official")

	ok := hc.processHealthCheck(node)
	if !ok {
		t.Fatalf("expected health check ok")
	}
	if gotMethod != "getLatestBlock" {
		t.Fatalf("expected method getLatestBlock, got %q", gotMethod)
	}
}

func TestCheckNodeHealth_SetsSourceFromTargetNode(t *testing.T) {
	// Ensure the plumbing from discovery.TargetNode.Source -> lbnode.Node.Source()
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req rpcReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		gotMethod = req.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":"0x2","id":1}`))
	}))
	defer srv.Close()

	host, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := net.LookupPort("tcp", portStr)
	if err != nil {
		t.Fatalf("LookupPort(%q): %v", portStr, err)
	}

	hc := NewNodeHealthChecker(2*time.Second, 2*time.Second)
	tn := &discovery.TargetNode{ChainId: "1", NodeKey: "n1", Address: host, Port: port, Weight: 1, NodeType: discovery.NodeTypeState, Source: "native", StateType: 1}

	n, err := hc.CheckNodeHealth(context.Background(), tn)
	if err != nil {
		t.Fatalf("expected CheckNodeHealth ok, got err: %v", err)
	}
	if n.Source() != "native" {
		t.Fatalf("expected node source native, got %q", n.Source())
	}
	if gotMethod != "eth_blockNumber" {
		t.Fatalf("expected method eth_blockNumber, got %q", gotMethod)
	}
}
