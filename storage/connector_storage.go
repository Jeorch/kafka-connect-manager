package storage

type ConnectorStorage interface {
	PutOne(tag string, connector string) (err error)
	RemoveOne(tag string, connector string) (err error)
	RemoveAll(tag string) (err error)
	List(tag string) (connectors []string, err error)
}
