package usage

const (
	// Topic is the fixed Kafka topic for usage records.
	Topic = "leafage-usage"

	// UnknownClientID is used when usage collection is enabled but the request
	// does not contain a non-blank client-id header.
	UnknownClientID = "unknown"
)

// Record is one locally aggregated usage message written to Kafka.
type Record struct {
	ID        string `json:"id"`
	ClientID  string `json:"client_id"`
	TimeMS    int64  `json:"time_ms"`
	ChainID   int64  `json:"chain_id"`
	Timestamp int64  `json:"timestamp"`
}
