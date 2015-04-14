package gorientdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Driver interface {
	connect() error
	Disconnect()
	DatabaseInfo() (interface{}, error)
	Command(sql string) QueryResult
	Query(sql string) QueryResult
	GetCount(klass string) int
}

type OrientDbDriver struct {
	Config OrientConfig
	Client *http.Client
}

func NewDriver(config OrientConfig) *OrientDbDriver {
	client := &OrientDbDriver{
		config,
		&http.Client{},
	}

	return client
}

func (d *OrientDbDriver) connect() error {
	fmt.Println("Executing database connect")

	method := fmt.Sprintf("connect/%s", d.Config.Database)
	req, err := http.NewRequest("GET", d.getUrl(method), nil)
	if err != nil {
        return err
	}

	d.auth(req)

	_, err = d.processRequest(req, 0)
    return err
}

func (d *OrientDbDriver) Disconnect() {
	fmt.Println("Executing database disconnect")

	req, err := http.NewRequest("GET", d.getUrl("disconnect"), nil)
	if err != nil {
        panic(err)
	}

	d.processRequest(req, 0)
}

func (d *OrientDbDriver) DatabaseInfo() (interface{}, error) {
	fmt.Println("Executing get database info")

	method := fmt.Sprintf("database/%s", d.Config.Database)
	req, err := http.NewRequest("GET", d.getUrl(method), nil)
	if err != nil {
        return nil, err
	}

	d.auth(req)

	data, err := d.processRequest(req, 0)
    var i interface{}
	if err == nil {
		json.Unmarshal(data, &i)
        return i, nil
	} else {
		return i, err
	}
}

func (d *OrientDbDriver) Command(sql string) (QueryResult, error) {
	fmt.Println("Executing command", sql)

	method := fmt.Sprintf("command/%s/sql", d.Config.Database)
	req, err := http.NewRequest("POST", d.getUrl(method), bytes.NewBufferString(sql))
	if err != nil {
		panic(err)
	}

	d.auth(req)

	data, err := d.processRequest(req, 0)
    var i QueryResult
	if err == nil {
		json.Unmarshal(data, &i)
        return i, nil
	} else {
		return i, err
	}
}

func (d *OrientDbDriver) Query(sql string) (QueryResult, error) {
	fmt.Println("Executing query", sql)

	method := fmt.Sprintf("/query/%s/sql", d.Config.Database)
	req, err := http.NewRequest("GET", d.getUrl(method), nil)
	if err != nil {
		panic(err)
	}

	d.auth(req)

	data, err := d.processRequest(req, 0)
    var i QueryResult
	if err == nil {
		json.Unmarshal(data, &i)
        return i, nil
	} else {
		return i, err
	}
}

// class can be a class or cluster:<clustername> notation
func (d *OrientDbDriver) GetCount(class string, cluster bool) uint64 {
	var sql string
	if cluster {
		sql = fmt.Sprintf("select count(*) from cluster:%s", class)
	} else {
		sql = fmt.Sprintf("select count(*) from %s", class)
	}

	result, err := d.Command(sql)
	if err == nil {
		if count, ok := result.Results[0].Properties["count"].(float64); ok {
			return uint64(count)
		} else {
			return 0
		}
	} else {
		return 0
	}
}

func (d *OrientDbDriver) processRequest(request *http.Request, bodySize int) ([]byte, error) {
	request.Header.Add("Accept-Encoding", "gzip,deflate")
	request.Header.Add("Content-Length", fmt.Sprintf("%d", bodySize))
	request.Header.Add("Content-Type", "application/json")

	resp, err := d.Client.Do(request)
	if err != nil {
		return make([]byte, 0), err
	}

	defer resp.Body.Close()
	bodyData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return make([]byte,0), err
	}

	// If there was no response, return nil to indicate no error
	if len(bodyData) == 0 {
		return nil, nil
	} else {
		return bodyData, nil
	}
}

func (d *OrientDbDriver) getUrl(method string) string {
	return fmt.Sprintf("http://%s:%d/%s", d.Config.ServerRoot, d.Config.Port, method)
}

func (d *OrientDbDriver) auth(request *http.Request) {
	request.SetBasicAuth(d.Config.Username, d.Config.Password)
}
