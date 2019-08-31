package utils

import "os"

func SetProjectEnv() {
	//connect config
	_ = os.Setenv("BP_KAFKA_CONNECT_URL", "http://192.168.100.176:8083")
	//redis config
	_ = os.Setenv("BM_REDIS_HOST", "192.168.100.176")
	_ = os.Setenv("BM_REDIS_PORT", "6379")
	_ = os.Setenv("BM_REDIS_PASS", "")
	_ = os.Setenv("BM_REDIS_DB", "0")
	//log config
	_ = os.Setenv("LOGGER_DEBUG", "true")
	//kafka config
	_ = os.Setenv("BM_KAFKA_BROKER", "123.56.179.133:9092")
	_ = os.Setenv("BM_KAFKA_SCHEMA_REGISTRY_URL", "http://123.56.179.133:8081")
	_ = os.Setenv("BM_KAFKA_CONSUMER_GROUP", "test20190828")
	_ = os.Setenv("BM_KAFKA_CA_LOCATION", "/Users/jeorch/kit/kafka-secrets/snakeoil-ca-1.crt")
	_ = os.Setenv("BM_KAFKA_CA_SIGNED_LOCATION", "/Users/jeorch/kit/kafka-secrets/kafkacat-ca1-signed.pem")
	_ = os.Setenv("BM_KAFKA_SSL_KEY_LOCATION", "/Users/jeorch/kit/kafka-secrets/kafkacat.client.key")
	_ = os.Setenv("BM_KAFKA_SSL_PASS", "pharbers")
}
