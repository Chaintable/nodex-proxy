package jsonrpc

func TooManyRequestsResponse() (*ResponseObject, error) {
	return ErrorResponse(&ErrorObject{
		Code:    ExtTooManyRequests,
		Message: ExtTooManyRequestsMsg,
	}, nil)
}

func BadRequest(err error) (*ResponseObject, error) {
	return ErrorResponse(&ErrorObject{
		Code:    ParseErrorCode,
		Message: ParseErrorMsg,
		Data:    err.Error(),
	}, err)
}

func BadGateway(err error) (*ResponseObject, error) {
	return ErrorResponse(&ErrorObject{
		Code:    InternalErrorCode,
		Message: ServerErrorMsg,
		Data:    err.Error(),
	}, err)
}

func GatewayTimeout(err error) (*ResponseObject, error) {
	return ErrorResponse(&ErrorObject{
		Code:    ExtWaitingTargetResponseTimeout,
		Message: ServerErrorMsg,
		Data:    err.Error(),
	}, err)
}

// ErrorResponse builds a JSON-RPC error response object with id 1. The
// returned error is err, or errorObject when err is nil.
func ErrorResponse(errorObject *ErrorObject, err error) (*ResponseObject, error) {
	respObject := &ResponseObject{
		Jsonrpc: "2.0",
		Error:   errorObject,
		Result:  nil,
		ID:      []byte("1"),
	}
	if err == nil {
		err = errorObject
	}
	return respObject, err
}
