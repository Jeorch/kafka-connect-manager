package schema

type Schema struct {
	Type string `json:"type"`
	Name string `json:"name"`
	Namespace string `json:"namespace"`
	Fields []map[string]interface{} `json:"fields"`
}
