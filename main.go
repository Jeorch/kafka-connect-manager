package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/PharbersDeveloper/kafka-connect-manager/manager"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
	"strings"
	"time"
)

func main() {
	fmt.Println("kafka_connect_manager start")

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

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"ConnectRequest"}
	bkc.SubscribeTopics(topics, dealConnectRequest)

}

func dealConnectRequest(a interface{}) {

	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	decoder := kafkaAvro.NewKafkaAvroDecoder(schemaRepositoryUrl)
	decodeData, err := decoder.Decode(a.([]byte))
	record := decodeData.(*avro.GenericRecord)
	bmerror.PanicError(err)
	fmt.Println("ConnectRequest => ", record)

	jobId := record.Get("jobId").(string)
	tag := record.Get("tag").(string)
	configs := record.Get("configs").([]interface{})
	configsStr := make([]string, 0)
	for _, config := range configs {
		if cs, ok := config.(string); ok {
			configsStr = append(configsStr, cs)
		}
	}

	go applyConnector(jobId, tag, configsStr)
	fmt.Println("dealConnectRequest DONE!")
}

func applyConnector(jobId string, tag string, configs []string) (err error) {

	bmlog.StandardLogger().Infof("jobId=%s, tag=%s, configs=%v", jobId, tag, configs)

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

	var configMap map[string]interface{}
	if err := json.Unmarshal([]byte(configs[0]), &configMap); err != nil {
		return err
	}

	connectorName := fmt.Sprintf("%s$$%s", availableConnectors[0], availableConnectors[1])
	sourceTopic := ""
	recallTopic := fmt.Sprintf("recall_%s", jobId)
	strategy := "default"

	if topic, ok := configMap["topic"]; ok {
		sourceTopic = topic.(string)
	} else if topics, ok := configMap["topics"]; ok {
		sourceTopic = topics.(string)
	} else {
		bmerror.PanicError(errors.New("no topic found in config"))
	}

	doMonitor(jobId, connectorName, sourceTopic, recallTopic, strategy)
	
	return
}

func doMonitor(jobId string, connectorName string, sourceTopic string, recallTopic string, strategy string)  {

	bmlog.StandardLogger().Info("*** doMonitor ***")
	sendMonitorRequest2(jobId, connectorName, sourceTopic, recallTopic, strategy)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"MonitorResponse2"}
	bkc.SubscribeTopics(topics, dealMonitorResponse2)
}

func sendMonitorRequest2(jobId string, connectorName string, sourceTopic string, recallTopic string, strategy string) {

	bmlog.StandardLogger().Info("*** sendMonitorRequest2 ***")
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	var rawMetricsSchema = `{"type": "record","name": "MonitorRequest2","namespace": "com.pharbers.kafka.schema","fields": [{"name": "jobId", "type": "string"},{"name": "connectorName", "type": "string"},{"name": "sourceTopic", "type": "string"},{"name": "recallTopic", "type": "string"},{"name": "strategy", "type": "string"}]}`

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	schema, err := avro.ParseSchema(rawMetricsSchema)
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(schema)
	bmerror.PanicError(err)
	record.Set("jobId", jobId)
	record.Set("connectorName", connectorName)
	record.Set("sourceTopic", sourceTopic)
	record.Set("recallTopic", recallTopic)
	record.Set("strategy", strategy)
	recordByteArr, err := encoder.Encode(record)
	bmerror.PanicError(err)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topic := "MonitorRequest2"
	bkc.Produce(&topic, recordByteArr)

}

func dealMonitorResponse2(a interface{}) {

	bmlog.StandardLogger().Info("*** dealMonitorResponse2 ***")
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	decoder := kafkaAvro.NewKafkaAvroDecoder(schemaRepositoryUrl)
	decodeData, err := decoder.Decode(a.([]byte))
	record := decodeData.(*avro.GenericRecord)
	bmerror.PanicError(err)
	fmt.Println("MonitorResponse2 => ", record)

	jobId := record.Get("jobId").(string)
	progress := record.Get("progress").(int64)
	connectorName := record.Get("connectorName").(string)
	error := record.Get("error").(string)

	if error != "" {
		bmerror.PanicError(errors.New(error))
	}

	if progress == 100 {
		connectors := strings.Split(connectorName, "$$")
		err = manager.ReleaseConnector(connectors[0])
		bmerror.PanicError(err)
		err = manager.ReleaseConnector(connectors[1])
		bmerror.PanicError(err)
	}

	bmlog.StandardLogger().Infof("jobId=%s, progress=%d, connectorName=%s, error=%s", jobId, progress, connectorName, error)

	replyFunc(jobId, progress)
	fmt.Println("dealMonitorResponse2 DONE!")
}

func replyFunc(jobId string, progress int64)  {

	bmlog.StandardLogger().Info("*** replyFunc *** ", jobId, " => ", progress)
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	var rawMetricsSchema = `{"type": "record","name": "ConnectResponse","namespace": "com.pharbers.kafka.schema","fields": [{"name": "jobId", "type": "string"},{"name": "progress", "type": "long"}]}`

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	schema, err := avro.ParseSchema(rawMetricsSchema)
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(schema)
	bmerror.PanicError(err)
	record.Set("jobId", jobId)
	record.Set("progress", progress)
	recordByteArr, err := encoder.Encode(record)
	bmerror.PanicError(err)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topic := "ConnectResponse"
	bkc.Produce(&topic, recordByteArr)

	//Todo release connector

}
