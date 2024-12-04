package types

import "github.com/Chaintable/nodex-proxy/jsonrpc"

type RPCMethodHandlerI interface {
	PreHandlerMap() map[jsonrpc.RPCMethod]PreProcessorFunc
	PostHandlerMap() map[jsonrpc.RPCMethod]PostProcessorFunc
}
