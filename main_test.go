package main

import (
	"encoding/json"
	"fmt"
	"github.com/PharbersDeveloper/kafka-connect-manager/utils"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
	"testing"
)

func TestSendConnectRequest(t *testing.T) {

	jobId := "test003"

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

	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")

	fields := make([]map[string]interface{}, 0)

	fields = append(fields,
		map[string]interface{}{
			"name" : "jobId",
			"type" : "string",
		},
		map[string]interface{}{
			"name" : "tag",
			"type" : "string",
		},
		map[string]interface{}{
			"name" : "configs",
			"type" : map[string]interface{}{
				"type" : "array",
				"items" : "string",
			},
		},
	)

	schemaConnectRequest := utils.Schema{
		Type:      "record",
		Name:      "ConnectRequest",
		Namespace: "com.pharbers.kafka.schema",
		Fields:    fields,
	}

	//var rawMetricsSchema = `{"type": "record","name": "ConnectRequest","namespace": "com.pharbers.kafka.schema","fields": [{"name": "jobId", "type": "string"},{"name": "tag", "type": "string"},{"name": "configs", "type": "{\"type\": \"array\", \"items\": \"string\"}"}]}`
	schemaByte, err := json.Marshal(schemaConnectRequest)
	rawMetricsSchema := string(schemaByte)
	bmerror.PanicError(err)

	tmpTopic := jobId
	bmlog.StandardLogger().Infof("jobId=%s", jobId)
	bmlog.StandardLogger().Infof("tmpTopic=%s", tmpTopic)
	tag := "tm"
	configs := []string{
		fmt.Sprintf(`{
	"connector.class": "com.pharbers.kafka.connect.mongodb.MongodbSourceConnector",
	"tasks.max": "1",
	"job": "%s",
	"topic": "%s",
	"connection": "mongodb://192.168.100.176:27017",
	"database": "test",
	"collection": "PhAuth",
	"filter": "{}"
}`, jobId, tmpTopic),
		fmt.Sprintf(`{
  	"jobId": "%s",
	"topics": "%s",
  	"connector.class": "com.pharbers.kafka.connect.elasticsearch.ElasticsearchSinkConnector",
  	"tasks.max": "1",
  	"key.ignore": "true",
 	 "connection.url": "http://59.110.31.215:9200",
  	"type.name": "",
  	"read.timeout.ms": "10000",
  	"connection.timeout.ms": "5000"
}`, jobId, tmpTopic),
	}

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	schema, err := avro.ParseSchema(rawMetricsSchema)
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(schema)
	bmerror.PanicError(err)
	record.Set("jobId", jobId)
	record.Set("tag", tag)
	record.Set("configs", configs)
	recordByteArr, err := encoder.Encode(record)
	bmerror.PanicError(err)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topic := "ConnectRequest"
	bkc.Produce(&topic, recordByteArr)
}