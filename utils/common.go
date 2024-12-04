package utils

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"

	"github.com/Chaintable/nodex-proxy/lib/log"
	"github.com/andybalholm/brotli"
)

func getReaderData(reader io.ReadCloser) ([]byte, io.ReadCloser, error) {
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, err
	}
	defer func(reader io.ReadCloser) {
		_ = reader.Close()

	}(reader)
	return body, io.NopCloser(bytes.NewBuffer(body)), nil
}

func ReadBodyDataFromHTTPResponse(resp *http.Response) ([]byte, error) {
	var (
		err  error
		body []byte
	)
	body, resp.Body, err = getReaderData(resp.Body)
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		log.Debug("The response is gzip encoded")
		gReader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		body, err = io.ReadAll(gReader)
		if err != nil {
			return nil, err
		}
	case "br":
		log.Debug("The response is brotli encoded")
		body, err = io.ReadAll(brotli.NewReader(bytes.NewReader(body)))
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return body, nil
}
