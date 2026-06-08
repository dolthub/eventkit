package eventkit

import "encoding/json"

type Codec interface {
	Marshal(*LogEventsRequest) ([]byte, error)
	Unmarshal([]byte, *LogEventsRequest) error
}

type JSONCodec struct{}

func (JSONCodec) Marshal(req *LogEventsRequest) ([]byte, error) {
	return json.Marshal(req)
}

func (JSONCodec) Unmarshal(data []byte, req *LogEventsRequest) error {
	return json.Unmarshal(data, req)
}

var DefaultCodec Codec = JSONCodec{}
