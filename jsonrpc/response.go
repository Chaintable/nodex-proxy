package jsonrpc

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"

	json "github.com/bytedance/sonic"
)

func TooManyRequestsResponse() (resp *http.Response, respObject *ResponseObject, err error) {
	resp, respObject, err = ErrorResponse(http.StatusTooManyRequests, &ErrorObject{
		Code:    ExtTooManyRequests,
		Message: ExtTooManyRequestsMsg,
	}, nil)
	resp.Header.Add("Retry-After", "10")
	return
}

func BadRequest(err error) (*http.Response, *ResponseObject, error) {
	return ErrorResponse(http.StatusBadRequest, &ErrorObject{
		Code:    ParseErrorCode,
		Message: ParseErrorMsg,
		Data:    err.Error(),
	}, err)
}

func BadGateway(err error) (*http.Response, *ResponseObject, error) {
	return ErrorResponse(http.StatusBadGateway, &ErrorObject{
		Code:    InternalErrorCode,
		Message: ServerErrorMsg,
		Data:    err.Error(),
	}, err)
}

func GatewayTimeout(err error) (*http.Response, *ResponseObject, error) {
	return ErrorResponse(http.StatusGatewayTimeout, &ErrorObject{
		Code:    ExtWaitingTargetResponseTimeout,
		Message: ServerErrorMsg,
		Data:    err.Error(),
	}, err)
}

func InternalServerError(err error) (*http.Response, *ResponseObject, error) {
	return ErrorResponse(http.StatusInternalServerError, &ErrorObject{
		Code:    InternalErrorCode,
		Message: ServerErrorMsg,
		Data:    err.Error(),
	}, err)
}

// ErrorResponse returns a JSON response containing jsonrpc.ErrorObject.
func ErrorResponse(httpCode int, errorObject *ErrorObject, err error) (*http.Response, *ResponseObject, error) {
	respObject := &ResponseObject{
		Jsonrpc: "2.0",
		Error:   errorObject,
		Result:  nil,
		ID:      []byte("1"),
	}
	if err == nil {
		err = errorObject
	}
	resp, respErr := Response(httpCode, respObject)
	if respErr != nil {
		err = respErr
	}
	return resp, respObject, err
}

// Response returns a JSON response containing jsonrpc.ResponseObject, or a plaintext generic
// response for this httpCode and an error when JSON marshalling fails.
func Response(httpCode int, resp *ResponseObject) (*http.Response, error) {
	body, err := json.Marshal(resp)
	if err != nil {
		return &http.Response{
			Body:       io.NopCloser(strings.NewReader(http.StatusText(httpCode))),
			StatusCode: httpCode,
			Header:     http.Header{},
		}, fmt.Errorf("failed to serialize jsonrpc response object: %v", err)
	}
	header := http.Header{}
	header.Add("Content-type", "application/json")
	return &http.Response{
		Body:       io.NopCloser(bytes.NewReader(body)),
		StatusCode: httpCode,
		Header:     header,
	}, nil
}
