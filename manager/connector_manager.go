package manager

import (
	"github.com/PharbersDeveloper/kafka-connect-manager/operations"
	"github.com/PharbersDeveloper/kafka-connect-manager/storage"
)

func RegisterConnector(tag string, connector string, config string) (err error) {
	err = operations.PutConnector(connector, config)
	if err != nil {
		return
	}
	rsi := storage.RedisStorageInstance
	err = rsi.PutOne(tag, connector)
	if err != nil {
		return
	}
	defaultStatus := "pause"
	err = operations.SetStatus(connector, defaultStatus)
	return
}

func RemoveConnector(tag string, connector string) (err error) {
	err = operations.DelConnector(connector)
	if err != nil {
		return
	}
	rsi := storage.RedisStorageInstance
	err = rsi.RemoveOne(tag, connector)
	return
}

func ListAllConnectors(tag string) (connectors []string, err error) {
	rsi := storage.RedisStorageInstance
	connectors, err = rsi.List(tag)
	return
}

func AvailableConnectors(tag string) (availableConnectors []string, err error) {
	rsi := storage.RedisStorageInstance
	connectors, err := rsi.List(tag)
	if err != nil {
		return
	}
	for _, connector := range connectors {
		status, err := operations.GetStatus(connector)
		if err != nil {
			return nil, err
		}
		if status == "PAUSED" {
			availableConnectors = append(availableConnectors, connector)
		}
	}

	return
}

func ResumeConnector(connector string, config string) (err error) {
	err = operations.SetConfig(connector, config)
	if err != nil {
		return
	}
	err = operations.SetStatus(connector, "resume")
	return
}

func ReleaseConnector(connector string) (err error) {
	err = operations.SetStatus(connector, "pause")
	return
}

func RegisterConnectorPlugin(connectServerAdmin string, connectServerHost string, pluginPath string) (err error) {
	return
}
