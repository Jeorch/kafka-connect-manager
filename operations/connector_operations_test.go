package operations

import (
	"github.com/alfredyang1986/blackmirror/bmerror"
	"os"
	"testing"
)

func TestPutConnector(t *testing.T) {

	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	_ = os.Setenv("LOGGER_DEBUG", "true")
	connector := "tm-001"
	config :=
		`{
        "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
        "tasks.max": "1",
        "topic": "tm-source-002",
        "file": "/usr/share/logs-path/test2222222.txt"
    }`
	err := PutConnector(connector, config)
	bmerror.PanicError(err)

}

func TestDelConnector(t *testing.T) {

	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	_ = os.Setenv("LOGGER_DEBUG", "true")
	connector := "tm-001"
	err := DelConnector(connector)
	bmerror.PanicError(err)

}
