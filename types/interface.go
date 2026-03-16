package types

import "github.com/Chaintable/nodex-proxy/jsonrpc"

type RPCMethodHandlerIHertz interface {
	PreHandlerMap() map[jsonrpc.RPCMethod]ProcessorFuncHertz
	PostHandlerMap() map[jsonrpc.RPCMethod]ProcessorFuncHertz
}
