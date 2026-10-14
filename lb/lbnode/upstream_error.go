package lbnode

import (
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	errs "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/netpoll"
)

// upstreamErrorHandler replaces reverseproxy's default handler, which answers
// 502 for every upstream client error. A read timeout means the backend took
// the request but did not answer within the configured RPC timeout, so it is
// reported as 504; failing to reach the backend at all (dial errors, closed
// connections, an exhausted connection pool) stays 502. Callers can then tell
// a slow request apart from an unreachable backend.
//
// The response is reset first: a timeout in the middle of the body leaves the
// partially read upstream headers and body in it, and the JSON-RPC error
// written afterwards must not be appended to them.
func upstreamErrorHandler(c *app.RequestContext, err error) {
	c.Response.Reset()
	if IsUpstreamReadTimeout(err) {
		c.Response.Header.SetStatusCode(consts.StatusGatewayTimeout)
		return
	}
	c.Response.Header.SetStatusCode(consts.StatusBadGateway)
}

// IsUpstreamReadTimeout reports whether err is the upstream read timeout.
// hertz normalizes a timeout while waiting for the first response byte to
// errs.ErrTimeout, but a timeout while reading the headers or body surfaces as
// the raw netpoll errno, so both are checked. Dial timeouts are excluded: they
// mean the backend could not be reached.
func IsUpstreamReadTimeout(err error) bool {
	return errors.Is(err, errs.ErrTimeout) || errors.Is(err, netpoll.ErrReadTimeout)
}
