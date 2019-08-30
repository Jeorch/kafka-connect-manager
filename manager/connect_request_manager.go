package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
	"time"
)

func (m *Manager) DealConnectRequest() {
	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"ConnectRequest"}
	bkc.SubscribeTopics(topics, m.dealConnectRequestFunc)

}

func (m *Manager) dealConnectRequestFunc(a interface{}) {

	go func() {
		var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
		decoder := kafkaAvro.NewKafkaAvroDecoder(schemaRepositoryUrl)
		decodeData, err := decoder.Decode(a.([]byte))
		record := decodeData.(*avro.GenericRecord)
		bmerror.PanicError(err)
		bmlog.StandardLogger().Infof("ConnectRequest => %v", record)

		jobId := record.Get("JobId").(string)
		tag := record.Get("Tag").(string)
		sourceConfig := record.Get("SourceConfig").(string)
		sinkConfig := record.Get("SinkConfig").(string)

		go m.applyConnector(jobId, tag, sourceConfig, sinkConfig)
	}()

}

func (m *Manager) applyConnector(jobId string, tag string, sourceConfig string, sinkConfig string) {

	bmlog.StandardLogger().Infof("applyConnector => jobId=%s, tag=%s", jobId, tag)

	availableConnectors, err := AvailableConnectors(tag)
	bmerror.PanicError(err)

	for len(availableConnectors)<2 {
		bmlog.StandardLogger().Warn(fmt.Sprintf("可用管道数量小于2，目前标签为%s的可用管道数量为%d，排队等待，每2秒轮询一次", tag, len(availableConnectors)))
		time.Sleep(2 * time.Second)
		availableConnectors, err = AvailableConnectors(tag)
		bmerror.PanicError(err)
	}

	err = ResumeConnector(availableConnectors[0], sourceConfig)
	if err != nil {
		err = ReleaseConnector(availableConnectors[0])
		bmerror.PanicError(err)
	}
	err = ResumeConnector(availableConnectors[1], sinkConfig)
	if err != nil {
		err = ReleaseConnector(availableConnectors[1])
		bmerror.PanicError(err)
	}

	if  m.jobConnectors == nil{
		m.jobConnectors = make(map[string][]string)
	}
	m.jobConnectors[jobId] = []string{availableConnectors[0], availableConnectors[1]}

	var configMap map[string]interface{}
	if err := json.Unmarshal([]byte(sourceConfig), &configMap); err != nil {
		panic(err.Error())
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

	go sendMonitorRequest2(jobId, connectorName, sourceTopic, recallTopic, strategy)

	return
}

func sendConnectResponse(jobId string, progress int64, error string)  {

	bmlog.StandardLogger().Infof("sendConnectResponse => jobId=%s， progress=%d", jobId, progress)
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	var rawMetricsSchema = `{"type": "record","name": "ConnectResponse","namespace": "com.pharbers.kafka.schema","fields": [{"name": "JobId", "type": "string"},{"name": "Progress", "type": "long"},{"name": "Error", "type": "string"}]}`

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	schema, err := avro.ParseSchema(rawMetricsSchema)
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(schema)
	bmerror.PanicError(err)
	record.Set("JobId", jobId)
	record.Set("Progress", progress)
	record.Set("Error", error)
	recordByteArr, err := encoder.Encode(record)
	if err != nil {
		bmlog.StandardLogger().Errorf("%v", err)
	}

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		bmlog.StandardLogger().Errorf("%v", err)
	}
	topic := "ConnectResponse"
	bkc.Produce(&topic, recordByteArr)
	bmlog.StandardLogger().Infof("*** Send ConnectResponse *** jobId=%s， progress=%d, succeed !!", jobId, progress)
}