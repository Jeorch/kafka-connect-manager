package test

import (
	"encoding/json"
	"fmt"
	"github.com/PharbersDeveloper/kafka-connect-manager/schema"
	"github.com/PharbersDeveloper/kafka-connect-manager/utils"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
	"testing"
)

func TestSendConnectRequest(t *testing.T) {

	jobId := "test003"

	utils.SetProjectEnv()

	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")

	fields := make([]map[string]interface{}, 0)

	fields = append(fields,
		map[string]interface{}{
			"name" : "JobId",
			"type" : "string",
		},
		map[string]interface{}{
			"name" : "Tag",
			"type" : "string",
		},
		map[string]interface{}{
			"name" : "SourceConfig",
			"type" : "string",
		},
		map[string]interface{}{
			"name" : "SinkConfig",
			"type" : "string",
		},
	)

	schemaConnectRequest := schema.Schema{
		Type:      "record",
		Name:      "ConnectRequest",
		Namespace: "com.pharbers.kafka.schema",
		Fields:    fields,
	}

	schemaByte, err := json.Marshal(schemaConnectRequest)
	rawMetricsSchema := string(schemaByte)
	bmerror.PanicError(err)

	tmpTopic := jobId
	tag := "TM"
	sourceConfig := fmt.Sprintf(`{
	"connector.class": "com.pharbers.kafka.connect.mongodb.MongodbSourceConnector",
	"tasks.max": "1",
	"job": "%s",
	"topic": "%s",
	"connection": "mongodb://192.168.100.176:27017",
	"database": "test",
	"collection": "PhAuth",
	"filter": "{}"
}`, jobId, tmpTopic)
	sinkConfig := fmt.Sprintf(`{
  	"jobId": "%s",
	"topics": "%s",
  	"connector.class": "com.pharbers.kafka.connect.elasticsearch.ElasticsearchSinkConnector",
  	"tasks.max": "1",
  	"key.ignore": "true",
 	 "connection.url": "http://59.110.31.215:9200",
  	"type.name": "",
  	"read.timeout.ms": "10000",
  	"connection.timeout.ms": "5000"
}`, jobId, tmpTopic)

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	schema, err := avro.ParseSchema(rawMetricsSchema)
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(schema)
	bmerror.PanicError(err)
	record.Set("JobId", jobId)
	record.Set("Tag", tag)
	record.Set("SourceConfig", sourceConfig)
	record.Set("SinkConfig", sinkConfig)
	recordByteArr, err := encoder.Encode(record)
	bmerror.PanicError(err)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topic := "ConnectRequest"
	bkc.Produce(&topic, recordByteArr)
}
