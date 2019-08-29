package operations

import (
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"os"
	"testing"
)

func TestGetStatus(t *testing.T) {
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	_ = os.Setenv("LOGGER_DEBUG", "true")
	status, err := GetStatus("tm-sink-001")

	bmlog.StandardLogger().Info(status)
	bmerror.PanicError(err)

}

func TestSetStatus(t *testing.T) {
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	_ = os.Setenv("LOGGER_DEBUG", "true")
	err := SetStatus("tm-sink-001", "pause")
	//err := SetStatus("tm-sink-001", "resume", "")
	bmerror.PanicError(err)
}
