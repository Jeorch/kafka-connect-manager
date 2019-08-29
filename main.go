package main

import (
	"fmt"
	"github.com/PharbersDeveloper/kafka-connect-manager/manager"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
	"time"
)

func main() {
	fmt.Println("kafka_connect_manager start")

	_ = os.Setenv("BM_KAFKA_BROKER", "123.56.179.133:9092")
	_ = os.Setenv("BM_KAFKA_SCHEMA_REGISTRY_URL", "http://123.56.179.133:8081")
	_ = os.Setenv("BM_KAFKA_CONSUMER_GROUP", "test20190828")
	_ = os.Setenv("BM_KAFKA_CA_LOCATION", "/Users/jeorch/kit/kafka-secrets/snakeoil-ca-1.crt")
	_ = os.Setenv("BM_KAFKA_CA_SIGNED_LOCATION", "/Users/jeorch/kit/kafka-secrets/kafkacat-ca1-signed.pem")
	_ = os.Setenv("BM_KAFKA_SSL_KEY_LOCATION", "/Users/jeorch/kit/kafka-secrets/kafkacat.client.key")
	_ = os.Setenv("BM_KAFKA_SSL_PASS", "pharbers")

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"connect-request"}
	bkc.SubscribeTopics(topics, dealConnectRequest)

}

func dealConnectRequest(a interface{}) {
	fmt.Println("dealConnectRequest => ", a)
	//go applyConnector(jobID, tag, configs)
	fmt.Println("dealConnectRequest DONE!")
}

func applyConnector(jobID string, tag string, configs []string) (err error) {

	if len(configs) < 2 {
		bmlog.StandardLogger().Error("未收到一组管道配置")
	}

	availableConnectors, err := manager.AvailableConnectors(tag)
	bmerror.PanicError(err)

	for len(availableConnectors)<2 {
		bmlog.StandardLogger().Warn(fmt.Sprintf("可用管道数量小于2，目前标签为%s的可用管道数量为%d，排队等待，每2秒轮询一次", tag, len(availableConnectors)))
		time.Sleep(2 * time.Second)
		availableConnectors, err = manager.AvailableConnectors(tag)
		bmerror.PanicError(err)
	}
	
	err = manager.ResumeConnector(availableConnectors[0], configs[0])
	if err != nil {
		err = manager.ReleaseConnector(availableConnectors[0])
		bmerror.PanicError(err)
	}
	err = manager.ResumeConnector(availableConnectors[1], configs[1])
	if err != nil {
		err = manager.ReleaseConnector(availableConnectors[1])
		bmerror.PanicError(err)
	}

	doMonitor(jobID)
	
	return
}

func doMonitor(jobID string)  {

	sendMonitorRequest(jobID)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"MonitorResponse"}
	bkc.SubscribeTopics(topics, dealMonitorResponse)
}

func sendMonitorRequest(jobID string) {

	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	var rawMetricsSchema = `{"type": "record","name": "MonitorRequest","namespace": "com.pharbers.kafka.schema","fields": [{"name": "jobID", "type": "string"}]}`

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	schema, err := avro.ParseSchema(rawMetricsSchema)
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(schema)
	bmerror.PanicError(err)
	record.Set("jobID", jobID)
	recordByteArr, err := encoder.Encode(record)
	bmerror.PanicError(err)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topic := "MonitorRequest"
	bkc.Produce(&topic, recordByteArr)

}

func dealMonitorResponse(a interface{}) {
	fmt.Println("dealMonitorResponse => ", a)

	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	decoder := kafkaAvro.NewKafkaAvroDecoder(schemaRepositoryUrl)
	record, err := decoder.Decode(a.([]byte))
	bmerror.PanicError(err)
	fmt.Println("MonitorResponse => ", record.(*avro.GenericRecord))

	//replyFunc(jobID)
	fmt.Println("dealConnectRequest DONE!")
}

func replyFunc(jobID string)  {

}
