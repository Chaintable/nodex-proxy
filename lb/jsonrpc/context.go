package jsonrpc

import "context"

type jrpcxContextKeyType int

const (
	originHostKey jrpcxContextKeyType = iota

	// DBK HTTP metadata headers
	DBKBiz           = "x-dbk-biz"
	DBKSourceHost    = "x-dbk-source-host"
	DBKSource        = "x-dbk-source"
	DBKEnv           = "x-dbk-env"
	DBKServerVersion = "x-dbk-server-version"
)

func contextWithOriginHost(parent context.Context, originHost string) context.Context {
	return context.WithValue(parent, originHostKey, originHost)
}

func originHostFromContext(ctx context.Context, defaultHost string) string {
	if ctx == nil {
		return defaultHost
	}
	if originHost, ok := ctx.Value(originHostKey).(string); ok {
		return originHost
	}
	return defaultHost
}
