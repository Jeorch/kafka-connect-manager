package manager

import (
	"fmt"
	"github.com/PharbersDeveloper/kafka-connect-manager/operations"
	"github.com/PharbersDeveloper/kafka-connect-manager/storage"
	"github.com/alfredyang1986/blackmirror/bmlog"
)

type Manager struct {
	jobConnectors map[string][]string
}

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

func (m *Manager) ReleaseJobConnectors(jobId string) (err error) {
	if m.jobConnectors[jobId] == nil {
		return fmt.Errorf("jobId=%s没有在ManageJobMap中找到", jobId)
	}
	for _, connector := range m.jobConnectors[jobId] {
		err = ReleaseConnector(connector)
		if err != nil {
			bmlog.StandardLogger().Warnf("%v", err)
			bmlog.StandardLogger().Warnf("释放job=%s的管道%s失败，开始尝试删除管道并重建", jobId, connector)
			err = RebuildConnector(connector)
			if err != nil {
				return
			}
		}
	}
	delete(m.jobConnectors, jobId)
	return
}

func RebuildConnector(connector string) (err error) {

	config, err := operations.GetConfig(connector)
	if err != nil {
		bmlog.StandardLogger().Warnf("未能获得管道%s的config，原因是=>%v", connector, err)
		return
	}
	err = operations.DelConnector(connector)
	if err != nil {
		bmlog.StandardLogger().Warnf("未能删除管道%s，原因是=>%v", connector, err)
		return
	}
	err = operations.PutConnector(connector, config)
	if err != nil {
		bmlog.StandardLogger().Warnf("未能添加管道%s，原因是=>%v", connector, err)
		return
	}
	err = operations.SetStatus(connector, "pause")
	if err != nil {
		bmlog.StandardLogger().Warnf("未能将管道%s状态设置为pause，原因是=>%v", connector, err)
		return
	}

	return
}

func RegisterConnectorPlugin(connectServerAdmin string, connectServerHost string, pluginPath string) (err error) {
	return
}
