package types

import "testing"

func TestResponseErrorKeyAttemptLifetime(t *testing.T) {
	var p RequestContext
	body := []byte(`{"error":null}`)
	if !p.ContainsResponseErrorKey(body) {
		t.Fatal("expected error token")
	}
	// Response buffers may be reused by a retry. Pointer/length identity cannot
	// safely identify an attempt, so invalidation must be explicit.
	copy(body, `{"other":null}`)
	if !p.ContainsResponseErrorKey(body) {
		t.Fatal("same attempt should reuse the filter result")
	}
	p.ResetResponseErrorKey()
	if p.ContainsResponseErrorKey(body) {
		t.Fatal("retry should inspect replacement bytes")
	}
	copy(body, `{"Error":null}`)
	p.ResetResponseErrorKey()
	if !p.ContainsResponseErrorKey(body) {
		t.Fatal("retry must invalidate cached negatives too")
	}
}
