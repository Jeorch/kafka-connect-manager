// Code generated by github.com/actgardner/gogen-avro. DO NOT EDIT.
/*
 * SOURCES:
 *     ConnectRequest.avsc
 *     ConnectResponse.avsc
 *     MonitorRequest.avsc
 *     MonitorResponse.avsc
 *     TmAggRequest.avsc
 *     TmAggResponse.avsc
 */

package schema

import (
	"github.com/actgardner/gogen-avro/compiler"
	"github.com/actgardner/gogen-avro/container"
	"github.com/actgardner/gogen-avro/vm"
	"github.com/actgardner/gogen-avro/vm/types"
	"io"
)

type TmAggRequest struct {
	RequestId  string
	JobId      string
	ProposalId string
	ProjectId  string
	PeriodId   string
	Phase      int32
	Strategy   string
}

func NewTmAggRequestWriter(writer io.Writer, codec container.Codec, recordsPerBlock int64) (*container.Writer, error) {
	str := &TmAggRequest{}
	return container.NewWriter(writer, codec, recordsPerBlock, str.Schema())
}

func DeserializeTmAggRequest(r io.Reader) (*TmAggRequest, error) {
	t := NewTmAggRequest()

	deser, err := compiler.CompileSchemaBytes([]byte(t.Schema()), []byte(t.Schema()))
	if err != nil {
		return nil, err
	}

	err = vm.Eval(r, deser, t)
	return t, err
}

func NewTmAggRequest() *TmAggRequest {
	return &TmAggRequest{}
}

func (r *TmAggRequest) Schema() string {
	return "{\"fields\":[{\"name\":\"RequestId\",\"type\":\"string\"},{\"name\":\"JobId\",\"type\":\"string\"},{\"name\":\"ProposalId\",\"type\":\"string\"},{\"name\":\"ProjectId\",\"type\":\"string\"},{\"name\":\"PeriodId\",\"type\":\"string\"},{\"name\":\"Phase\",\"type\":\"int\"},{\"name\":\"Strategy\",\"type\":\"string\"}],\"name\":\"TmAggRequest\",\"namespace\":\"com.pharbers.kafka.schema\",\"type\":\"record\"}"
}

func (r *TmAggRequest) SchemaName() string {
	return "com.pharbers.kafka.schema.TmAggRequest"
}

func (r *TmAggRequest) Serialize(w io.Writer) error {
	return writeTmAggRequest(r, w)
}

func (_ *TmAggRequest) SetBoolean(v bool)    { panic("Unsupported operation") }
func (_ *TmAggRequest) SetInt(v int32)       { panic("Unsupported operation") }
func (_ *TmAggRequest) SetLong(v int64)      { panic("Unsupported operation") }
func (_ *TmAggRequest) SetFloat(v float32)   { panic("Unsupported operation") }
func (_ *TmAggRequest) SetDouble(v float64)  { panic("Unsupported operation") }
func (_ *TmAggRequest) SetBytes(v []byte)    { panic("Unsupported operation") }
func (_ *TmAggRequest) SetString(v string)   { panic("Unsupported operation") }
func (_ *TmAggRequest) SetUnionElem(v int64) { panic("Unsupported operation") }
func (r *TmAggRequest) Get(i int) types.Field {
	switch i {
	case 0:
		return (*types.String)(&r.RequestId)
	case 1:
		return (*types.String)(&r.JobId)
	case 2:
		return (*types.String)(&r.ProposalId)
	case 3:
		return (*types.String)(&r.ProjectId)
	case 4:
		return (*types.String)(&r.PeriodId)
	case 5:
		return (*types.Int)(&r.Phase)
	case 6:
		return (*types.String)(&r.Strategy)

	}
	panic("Unknown field index")
}
func (r *TmAggRequest) SetDefault(i int) {
	switch i {

	}
	panic("Unknown field index")
}
func (_ *TmAggRequest) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ *TmAggRequest) AppendArray() types.Field         { panic("Unsupported operation") }
func (_ *TmAggRequest) Finalize()                        {}

type TmAggRequestReader struct {
	r io.Reader
	p *vm.Program
}

func NewTmAggRequestReader(r io.Reader) (*TmAggRequestReader, error) {
	containerReader, err := container.NewReader(r)
	if err != nil {
		return nil, err
	}

	t := NewTmAggRequest()
	deser, err := compiler.CompileSchemaBytes([]byte(containerReader.AvroContainerSchema()), []byte(t.Schema()))
	if err != nil {
		return nil, err
	}

	return &TmAggRequestReader{
		r: containerReader,
		p: deser,
	}, nil
}

func (r *TmAggRequestReader) Read() (*TmAggRequest, error) {
	t := NewTmAggRequest()
	err := vm.Eval(r.r, r.p, t)
	return t, err
}
