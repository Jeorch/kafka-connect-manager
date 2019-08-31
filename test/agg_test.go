package test

import (
	"fmt"
	schema2 "github.com/PharbersDeveloper/kafka-connect-manager/schema"
	"github.com/PharbersDeveloper/kafka-connect-manager/utils"
	"github.com/alfredyang1986/blackmirror/bmerror"
	"github.com/alfredyang1986/blackmirror/bmkafka"
	"github.com/alfredyang1986/blackmirror/bmlog"
	"github.com/elodina/go-avro"
	kafkaAvro "github.com/elodina/go-kafka-avro"
	"os"
	"testing"
)

func TestSendAggRequest(t *testing.T) {

	utils.SetProjectEnv()

	bmlog.StandardLogger().Info("*** SendAggRequest ***")
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")

	//TmAggRequest("test", "5d57ed3cab0bf2192d416afb", "5d64fa55bfdb3821b0e31a78", "5d6500cabfdb3821b0e31b3d", 0, "", "Agg2Cal")
	aggRequest := schema2.TmAggRequest{
		RequestId: "test",
		JobId: "test",
		ProposalId: "5d57ed3cab0bf2192d416afb",
		ProjectId: "5d64fa55bfdb3821b0e31a78",
		PeriodId: "5d6500cabfdb3821b0e31b3d",
		Phase: 0,
		Strategy: "Agg2Cal",
	}

	encoder := kafkaAvro.NewKafkaAvroEncoder(schemaRepositoryUrl)
	valueSchema, err := avro.ParseSchema(aggRequest.Schema())
	bmerror.PanicError(err)
	record := avro.NewGenericRecord(valueSchema)
	record.Set("RequestId", aggRequest.RequestId)
	record.Set("JobId", aggRequest.JobId)
	record.Set("ProposalId", aggRequest.ProposalId)
	record.Set("ProjectId", aggRequest.ProjectId)
	record.Set("PeriodId", aggRequest.PeriodId)
	record.Set("Phase", aggRequest.Phase)
	record.Set("Strategy", aggRequest.Strategy)
	recordByteArr, err := encoder.Encode(record)
	bmerror.PanicError(err)

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topic := "TmAggRequest"
	bkc.Produce(&topic, recordByteArr)

}

func TestReceiveAggResponse(t *testing.T) {

	utils.SetProjectEnv()

	bkc, err := bmkafka.GetConfigInstance()
	if err != nil {
		panic(err.Error())
	}
	topics := []string{"TmAggResponse"}
	bkc.SubscribeTopics(topics, subscribeAvroFunc)

}

func subscribeAvroFunc(a interface{}) {
	fmt.Println("subscribeAvroFunc => ")
	var schemaRepositoryUrl = os.Getenv("BM_KAFKA_SCHEMA_REGISTRY_URL")
	decoder := kafkaAvro.NewKafkaAvroDecoder(schemaRepositoryUrl)
	record, err := decoder.Decode(a.([]byte))
	bmerror.PanicError(err)
	fmt.Println("TmAggResponse => ", record.(*avro.GenericRecord))
	fmt.Println("subscribeFunc DONE!")
}

func TestTemp(t *testing.T) {
	arr := []string{"a", "b", "c"}
	for i, item := range arr {
		if item == "b" {
			fmt.Println(arr[:i])
			fmt.Println(arr[i+1:])
			arr = append(arr[:i], arr[i+1:]...)
		}
	}
	fmt.Println(arr)
}
