package storage

import "github.com/alfredyang1986/blackmirror/bmredis"

type RedisStorage struct {
}

var RedisStorageInstance  = &RedisStorage{}

func (rs *RedisStorage) PutOne(tag string, connector string) (err error) {

	rc := bmredis.GetRedisClient()
	ic := rc.SAdd(tag, connector)
	err = ic.Err()
	return
}

func (rs *RedisStorage) RemoveOne(tag string, connector string) (err error) {
	rc := bmredis.GetRedisClient()
	ic := rc.SRem(tag, connector)
	err = ic.Err()
	return
}

func (rs *RedisStorage) RemoveAll(tag string) (err error) {
	rc := bmredis.GetRedisClient()
	ic := rc.Del(tag)
	err = ic.Err()
	return
}

func (rs *RedisStorage) List(tag string) (connectors []string, err error) {
	rc := bmredis.GetRedisClient()
	ssc := rc.SMembers(tag)
	err = ssc.Err()
	if err != nil {
		return
	}
	connectors, err = ssc.Result()
	return
}
