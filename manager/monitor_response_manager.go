package manager

import (
	"fmt"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
	"strings"
)

func (m *Manager) DealMonitorResponse() {
	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"MonitorResponse2"}
	bkc.SubscribeTopics(topics, m.dealMonitorResponse2)

}

func (m *Manager) dealMonitorResponse2(a interface{}) {

	bmlog.StandardLogger().Info("*** dealMonitorResponse2 ***")
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	decoder := kafkaAvro.NewKafkaAvroDecoder(schemaRepositoryUrl)
	decodeData, err := decoder.Decode(a.([]byte))
	record := decodeData.(*avro.GenericRecord)
	bmerror.PanicError(err)
	fmt.Println("MonitorResponse2 => ", record)

	jobId := record.Get("jobId").(string)
	if m.jobConnectors[jobId] != nil {
		progress := record.Get("progress").(int64)
		connectorName := record.Get("connectorName").(string)
		error := record.Get("error")

		if progress == -1 {
			bmlog.StandardLogger().Info("monitor error")
			replyFunc(jobId, progress, fmt.Sprintf("%v", error))
			bmlog.StandardLogger().Errorf("%v", error)
		}

		if error != nil && error != "" {
			bmlog.StandardLogger().Info("monitor error")
			replyFunc(jobId, progress, fmt.Sprintf("%v", error))
			bmlog.StandardLogger().Errorf("%v", error)
		}

		if progress == 100 {
			connectors := strings.Split(connectorName, "$$")
			err = ReleaseConnector(connectors[0])
			replyFunc(jobId, progress, fmt.Sprintf("%v", error))
			bmlog.StandardLogger().Errorf("%v", error)
			err = ReleaseConnector(connectors[1])
			replyFunc(jobId, progress, fmt.Sprintf("%v", error))
			bmlog.StandardLogger().Errorf("%v", error)
			delete(m.jobConnectors, jobId)
		}

		bmlog.StandardLogger().Infof("jobId=%s, progress=%d, connectorName=%s, error=%s", jobId, progress, connectorName, error)

		replyFunc(jobId, progress, fmt.Sprintf("%v", error))
		fmt.Println("dealMonitorResponse2 DONE!")
	}

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