package manager

import (
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"os"
	"testing"
)

func TestRegisterConnector(t *testing.T) {
	//connect config
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	//redis config
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	//log config
	_ = os.Setenv("LOGGER_DEBUG", "true")

	tag := "TM"
	connector := "TM002"
	config := `{
        "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
        "tasks.max": "1",
        "topic": "TM-source-002",
        "file": "/usr/share/logs-path/test2222222.txt"
    }`

	err := RegisterConnector(tag, connector, config)
	bmerror.PanicError(err)

}

func TestListAllConnectors(t *testing.T) {

	//connect config
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	//redis config
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	//log config
	_ = os.Setenv("LOGGER_DEBUG", "true")

	tag := "TM"
	connectors, err := ListAllConnectors(tag)
	bmerror.PanicError(err)
	bmlog.StandardLogger().Info(connectors)

}

func TestAvailableConnectors(t *testing.T) {
	//connect config
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	//redis config
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	//log config
	_ = os.Setenv("LOGGER_DEBUG", "true")

	tag := "TM"
	connectors, err := AvailableConnectors(tag)
	bmerror.PanicError(err)
	bmlog.StandardLogger().Info(connectors)
}

func TestResumeConnector(t *testing.T) {
	//connect config
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	//redis config
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	//log config
	_ = os.Setenv("LOGGER_DEBUG", "true")

	connector := "TM-002"
	config := `{
        "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
        "tasks.max": "1",
        "topic": "TM-source-002",
        "file": "/usr/share/logs-path/test33333.txt"
    }`

	err := ResumeConnector(connector, config)
	bmerror.PanicError(err)

}

func TestReleaseConnector(t *testing.T) {
	//connect config
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	//redis config
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	//log config
	_ = os.Setenv("LOGGER_DEBUG", "true")
	connector := "TM-002"

	err := ReleaseConnector(connector)
	bmerror.PanicError(err)
}

func TestRemoveConnector(t *testing.T) {

	//connect config
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	//redis config
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	//log config
	_ = os.Setenv("LOGGER_DEBUG", "true")

	tag := "TM"
	connector := "TM-001"

	err := RemoveConnector(tag, connector)
	bmerror.PanicError(err)

}
