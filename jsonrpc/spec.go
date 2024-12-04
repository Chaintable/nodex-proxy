// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package jsonrpc

import (
	"encoding/json"
	"fmt"
)

// Error codes
const (
	ParseErrorCode      ErrorCode = -32700
	InvalidRequestCode  ErrorCode = -32600
	MethodNotFoundCode  ErrorCode = -32601
	InvalidParamsCode   ErrorCode = -32602
	InternalErrorCode   ErrorCode = -32603
	InvalidResponseCode ErrorCode = -32604
	MethodExistsCode    ErrorCode = -32000
	URLSchemeErrorCode  ErrorCode = -32001
)

// Error message
const (
	ParseErrorMsg     ErrorMsg = "parse error"
	InvalidRequestMsg ErrorMsg = "invalid Request"
	MethodNotFoundMsg ErrorMsg = "method not found"
	InvalidParamsMsg  ErrorMsg = "invalid params"
	InternalErrorMsg  ErrorMsg = "internal error"
	ServerErrorMsg    ErrorMsg = "server error"
	MethodExistsMsg   ErrorMsg = "method exists"
	URLSchemeErrorMsg ErrorMsg = "url scheme error"
)

// Extend Error code and message.
const (
	ExtOutOfBlockQueryRange         ErrorCode = -41000
	ExtTooManyRequests              ErrorCode = -41001
	ExtWaitingTargetResponseTimeout ErrorCode = -41002
	ExtMethodNotAllowed             ErrorCode = -41003
	ExtTooManyRequestParams         ErrorCode = -41004
)

const (
	ExtOutOfBlockQueryRangeMsg ErrorMsg = "the height of queried block exceeds the limit"
	ExtTooManyRequestsMsg      ErrorMsg = "too many requests"
	ExtMethodNotAllowedMsg     ErrorMsg = "method not allowed"
	ExtTooManyRequestParamsMsg ErrorMsg = "the number of parameters cannot exceed one"
)

type RPCMethod string

const (
	EthBlockNumber                         RPCMethod = "eth_blockNumber"
	EthGetBlockByNumber                    RPCMethod = "eth_getBlockByNumber"
	EthGetBalance                          RPCMethod = "eth_getBalance"
	EthGetCode                             RPCMethod = "eth_getCode"
	EthGetStorageAt                        RPCMethod = "eth_getStorageAt"
	EthGetTransactionCount                 RPCMethod = "eth_getTransactionCount"
	EthGetBlockTransactionCountByNumber    RPCMethod = "eth_getBlockTransactionCountByNumber"
	EthGetUncleCountByBlockNumber          RPCMethod = "eth_getUncleCountByBlockNumber"
	EthGetTransactionByBlockNumberAndIndex RPCMethod = "eth_getTransactionByBlockNumberAndIndex"
	EthGetUncleByBlockNumberAndIndex       RPCMethod = "eth_getUncleByBlockNumberAndIndex"
	EthCall                                RPCMethod = "eth_call"
	EthMultiCall                           RPCMethod = "eth_multiCall"
	EthChainId                             RPCMethod = "eth_chainId"
	NetVersion                             RPCMethod = "net_version"
	EthGetLogs                             RPCMethod = "eth_getLogs"
)

// ErrorCode is a json rpc 2.0 error code.
type ErrorCode int

// ErrorMsg is a json rpc 2.0 error message.
type ErrorMsg string

// ErrorObject represents a response error object.
type ErrorObject struct {
	// Code indicates the error type that occurred.
	Code ErrorCode `json:"code"`
	// Message provides a short description of the error.
	Message ErrorMsg `json:"message"`
	// Data is a primitive or structured value that contains additional information.
	// about the error.
	Data interface{} `json:"data,omitempty"`
}

// RequestObject represents a request object
type RequestObject struct {
	// Jsonrpc specifies the version of the JSON-RPC protocol.
	// Must be exactly "2.0".
	Jsonrpc string `json:"jsonrpc"`
	// Method contains the name of the method to be invoked.
	Method RPCMethod `json:"method"`
	// Params is a structured value that holds the parameter values to be used during
	// the invocation of the method.
	Params json.RawMessage `json:"params"`
	// ID is a unique identifier established by the client.
	ID json.RawMessage `json:"id"`
}

// ResponseObject represents a response object.
type ResponseObject struct {
	// Jsonrpc specifies the version of the JSON-RPC protocol.
	// Must be exactly "2.0".
	Jsonrpc string `json:"jsonrpc"`
	// Error contains the error object if an error occurred while processing the request.
	Error *ErrorObject `json:"error,omitempty"`
	// Result contains the result of the called method.
	Result interface{} `json:"result,omitempty"`
	// ID contains the client established request id or null.
	ID json.RawMessage `json:"id"`
}

// Error implement error interface.
func (eo *ErrorObject) Error() string {
	return fmt.Sprintf("code: %v, err_msg: %v", eo.Code, eo.Message)
}
