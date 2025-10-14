package readiness

import (
	"fmt"
	"net/http"
	"time"
)

func CheckReadiness(address string, port int, path string, timeout time.Duration) bool {
	url := fmt.Sprintf("http://%s:%d%s", address, port, path)

	client := &http.Client{
		Timeout: timeout,
	}

	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
