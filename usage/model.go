package usage

const (
	// UnknownClientID is used when usage collection is enabled but the request
	// does not contain a non-blank client-id header.
	UnknownClientID = "unknown"

	ServiceLeafage   = "leafage"
	ResourceTypeRead = "read"
)

// Record is one locally aggregated usage message written to Kafka.
type Record struct {
	ID           string `json:"id"`
	ClientID     string `json:"client_id"`
	Service      string `json:"service"`
	ResourceType string `json:"resource_type"`
	Usage        int64  `json:"usage"`
	Timestamp    int64  `json:"timestamp"`
}
