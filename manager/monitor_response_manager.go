package manager

import (
	"fmt"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
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

	go func() {
		var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
		decoder := kafkaAvro.NewKafkaAvroDecoder(schemaRepositoryUrl)
		decodeData, err := decoder.Decode(a.([]byte))
		if err != nil {
			bmlog.StandardLogger().Errorf("%v", err)
		}
		record := decodeData.(*avro.GenericRecord)
		bmlog.StandardLogger().Infof("MonitorResponse2 => %s", record)

		jobId := record.Get("jobId").(string)
		if m.jobConnectors[jobId] != nil {
			progress := record.Get("progress").(int64)
			connectorName := record.Get("connectorName").(string)
			monitorError := record.Get("error")

			switch progress {
			case -1:
				bmlog.StandardLogger().Infof("jobId=%s, progress=%d, connectorName=%s, error=%v", jobId, progress, connectorName, monitorError)
				bmlog.StandardLogger().Info("monitor error")
				bmlog.StandardLogger().Errorf("%v", monitorError)
			case 100:
				bmlog.StandardLogger().Infof("jobId=%s, progress=%d, connectorName=%s, error=%v", jobId, progress, connectorName, monitorError)
				go sendConnectResponse(jobId, progress, fmt.Sprintf("%v", monitorError))
				err = m.ReleaseJobConnectors(jobId)
				if err != nil {
					bmlog.StandardLogger().Errorf("未能成功释放jobId=%s的管道，原因是=>%v", jobId, err)
				}
			default:
				bmlog.StandardLogger().Infof("jobId=%s, progress=%d, connectorName=%s, error=%v", jobId, progress, connectorName, monitorError)
			}

		}
	}()

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