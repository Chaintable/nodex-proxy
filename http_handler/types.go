package http_handler

type AddMethodRouteRequest struct {
	Method          string   `json:"method"`
	IncludeNodeKeys []string `json:"include_node_keys"`
	ExcludeNodeKeys []string `json:"exclude_node_keys"`
}

type AddMethodRouteResponse struct {
	Method          string   `json:"method"`
	IncludeNodeKeys []string `json:"include_node_keys"`
	ExcludeNodeKeys []string `json:"exclude_node_keys"`
	Error           string   `json:"error,omitempty"`
}

type RemoveMethodRouteRequest struct {
	Method          string   `json:"method"`
	IncludeNodeKeys []string `json:"include_node_keys"`
	ExcludeNodeKeys []string `json:"exclude_node_keys"`
}

type RemoveMethodRouteResponse struct {
	Method          string   `json:"method"`
	IncludeNodeKeys []string `json:"include_node_keys"`
	ExcludeNodeKeys []string `json:"exclude_node_keys"`
	Error           string   `json:"error,omitempty"`
}
