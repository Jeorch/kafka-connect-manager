package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	schema2 "github.com/PharbersDeveloper/kafka-connect-manager/schema"
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
		bmlog.StandardLogger().Warnf("可用管道数量小于2，目前标签为%s的可用管道数量为%d，排队等待，每2秒轮询一次", tag, len(availableConnectors))
		bmlog.StandardLogger().Warnf("当前jobConnectors=%v", m.jobConnectors)
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
	if m.jobConnectors[jobId] == nil {
		m.jobConnectors[jobId] = []string{availableConnectors[0], availableConnectors[1]}
	} else {
		m.jobConnectors[jobId] = append(m.jobConnectors[jobId], availableConnectors[0], availableConnectors[1])
	}


	var sourceConfigMap map[string]interface{}
	if err := json.Unmarshal([]byte(sourceConfig), &sourceConfigMap); err != nil {
		panic(err.Error())
	}

	var sinkConfigMap map[string]interface{}
	if err := json.Unmarshal([]byte(sinkConfig), &sinkConfigMap); err != nil {
		panic(err.Error())
	}

	connectorName := fmt.Sprintf("%s$$%s", availableConnectors[0], availableConnectors[1])
	var sourceTopic string
	var recallTopic string
	strategy := "default"

	if topic, ok := sourceConfigMap["topic"]; ok {
		sourceTopic = topic.(string)
	} else if topics, ok := sourceConfigMap["topics"]; ok {
		sourceTopic = topics.(string)
	} else {
		bmerror.PanicError(errors.New("no topic found in sourceConfigMap"))
	}

	if sinkJobID, ok := sinkConfigMap["jobId"]; ok {
		recallTopic = "recall_" +sinkJobID.(string)
	} else if sinkJobID, ok := sinkConfigMap["job"]; ok {
		recallTopic = "recall_" +sinkJobID.(string)
	} else {
		bmerror.PanicError(errors.New("no jobId found in sinkConfigMap"))
	}

	go sendMonitorRequest2(jobId, connectorName, sourceTopic, recallTopic, strategy)

	return
}

func sendConnectResponse(jobId string, progress int64, status string, msg string)  {

	bmlog.StandardLogger().Infof("sendConnectResponse => jobId=%s, progress=%d, status=%s， msg=%d", jobId, progress, status, msg)
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	connectResponse := schema2.ConnectResponse{
		JobId:    jobId,
		Progress: progress,
		Status:   status,
		Message:  msg,
	}
	var rawMetricsSchema = connectResponse.Schema()

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	schema, err := avro.ParseSchema(rawMetricsSchema)
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(schema)
	bmerror.PanicError(err)
	record.Set("JobId", connectResponse.JobId)
	record.Set("Progress", connectResponse.Progress)
	record.Set("Status", connectResponse.Status)
	record.Set("Message", connectResponse.Message)
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