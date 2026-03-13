package http_handler

type AddNodeRequest struct {
	NodeType int    `json:"node_type"`
	NodeKey  string `json:"node_key"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	State    int    `json:"state"`
	Weight   int    `json:"weight"`
}

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
