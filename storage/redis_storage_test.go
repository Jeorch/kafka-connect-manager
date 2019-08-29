package storage

import (
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"os"
	"testing"
)

func TestRedisStorage_PutOne(t *testing.T) {
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")

	tag := "tm-connectors"
	connector := "tm-001"

	err := RedisStorageInstance.PutOne(tag, connector)
	bmerror.PanicError(err)

}

func TestRedisStorage_List(t *testing.T) {

	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	_ = os.Setenv("LOGGER_DEBUG", "true")

	tag := "tm-connectors"

	connectors, err := RedisStorageInstance.List(tag)
	bmerror.PanicError(err)
	bmlog.StandardLogger().Info(connectors)

}

func TestRedisStorage_RemoveOne(t *testing.T) {

	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")

	tag := "tm-connectors"
	connector := "tm-001"

	err := RedisStorageInstance.RemoveOne(tag, connector)
	bmerror.PanicError(err)

}
