package lb

import (
	"net/http"
	"net/url"
)

func cloneURL(i *url.URL) *url.URL {
	out := new(url.URL)
	*out = *i

	if i.User != nil {
		u := *i.User
		out.User = &u
	}
	return out
}

func cloneRequest(r *http.Request) *http.Request {
	// shallow copy of the struct
	r2 := new(http.Request)
	*r2 = *r

	r2.URL = cloneURL(r.URL)

	// deep copy of the Header
	r2.Header = make(http.Header, len(r.Header))
	for k, s := range r.Header {
		r2.Header[k] = append([]string(nil), s...)
	}
	return r2
}
