package operations

import (
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"os"
	"testing"
)

func TestSetConfig(t *testing.T) {
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	_ = os.Setenv("LOGGER_DEBUG", "true")
	connectorName := "tm-sink-001"
	config :=
		`{
		"connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    	"tasks.max": "1",
    	"topic": "tm-source-001",
    	"file": "/usr/share/logs-path/test201.txt"
	}`
	err := SetConfig(connectorName, config)
	bmerror.PanicError(err)
}

func TestGetConfig(t *testing.T) {
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	_ = os.Setenv("LOGGER_DEBUG", "true")
	connectorName := "TM001"
	config, err := GetConfig(connectorName)
	bmerror.PanicError(err)
	bmlog.StandardLogger().Info(config)
}
