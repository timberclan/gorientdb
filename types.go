package gorientdb

import (
	"encoding/json"
	"gopkg.in/mgo.v2/bson"
)

type OrientConfig struct {
	ServerRoot string
	Port       uint
	Username   string
	Password   string
	Database   string
}

type Record struct {
	Properties map[string]interface{} `json:"-" bson:",inline"`
}

func (r *Record) MarshalJSON() ([]byte, error) {
	var j interface{}
	b, _ := bson.Marshal(r)
	bson.Unmarshal(b, &j)
	return json.Marshal(&j)
}

func (r *Record) UnmarshalJSON(b []byte) error {
	var j map[string]interface{}
	json.Unmarshal(b, &j)
	b, _ = bson.Marshal(&j)
	return bson.Unmarshal(b, r)
}

type QueryResult struct {
	Results []Record `json:"result"`
}
