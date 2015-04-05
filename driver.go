package gorientdb

import (
    "net/http"
    "fmt"
    "io/ioutil"
    "bytes"
    "encoding/json"
)

type OrientDbDriver struct {
    Config          OrientConfig
    Client          *http.Client
}

func NewDriver(config OrientConfig) (*OrientDbDriver) {
    client := &OrientDbDriver{
        config,
        &http.Client{},
    }

    return client
}

func (d *OrientDbDriver) connect() (error) {
    fmt.Println("Executing database connect")

    method := fmt.Sprintf("connect/%s", d.Config.Database)
    req, err := http.NewRequest("GET", d.getUrl(method), nil)
    if(err != nil) {
        panic(err)
    }

    d.auth(req)

    _, errChan := d.processRequest(req, 0)
    err = <- errChan
    return err
}

func (d *OrientDbDriver) Disconnect() {
    fmt.Println("Executing database disconnect")

    req, err := http.NewRequest("GET", d.getUrl("disconnect"), nil)
    if(err != nil){
        panic(err)
    }

    d.processRequest(req, 0)
}

func (d *OrientDbDriver) DatabaseInfo() (<- chan interface{}, <- chan error) {
    fmt.Println("Executing get database info")

    method := fmt.Sprintf("database/%s", d.Config.Database)
    req, err := http.NewRequest("GET", d.getUrl(method), nil)
    if(err != nil) {
        panic(err)
    }

    resultChan := make(chan interface{})
    errChan := make(chan error)

    go func(){
        d.auth(req)
        data, err := d.processRequest(req, 0)
        select {
        case result := <- data:
            var i interface{}
            json.Unmarshal(result, &i)
            resultChan <- i
        case fail := <- err:
            errChan <- fail
        }
    }()

    return resultChan, errChan
}

func (d *OrientDbDriver) Command(sql string) (<- chan QueryResult, <- chan error) {
    fmt.Println("Executing command", sql)

    method := fmt.Sprintf("command/%s/sql", d.Config.Database)
    req, err := http.NewRequest("POST", d.getUrl(method), bytes.NewBufferString(sql))
    if(err != nil){
        panic(err)
    }

    resultChan := make(chan QueryResult)
    errChan := make(chan error)

    go func(){
        d.auth(req)
        data, err := d.processRequest(req, 0)
        select {
        case result := <- data:
            var i QueryResult
            json.Unmarshal(result, &i)
            resultChan <- i
        case fail := <- err:
            errChan <- fail
        }
    }()

    return resultChan, errChan
}

func (d *OrientDbDriver) Query(sql string) (<- chan QueryResult, <-chan error){
    fmt.Println("Executing query", sql)

    method := fmt.Sprintf("/query/%s/sql", d.Config.Database)
    req, err := http.NewRequest("GET", d.getUrl(method), nil)
    if err != nil {
        panic(err)
    }

    d.auth(req)

    resultChan := make(chan QueryResult)
    errChan := make(chan error)

    go func() {

        data, err := d.processRequest(req, 0)
        select {
        case result := <- data:
            var i QueryResult
            json.Unmarshal(result, &i)
            resultChan <- i
        case fail := <- err:
            errChan <- fail
        }
    }()

    return resultChan,errChan
}

func (d *OrientDbDriver) processRequest(request *http.Request, bodySize int) (chan []byte, chan error) {
    dataReturn := make(chan []byte)
    errReturn := make(chan error)

    go func(){
        request.Header.Add("Accept-Encoding", "gzip,deflate")
        request.Header.Add("Content-Length", fmt.Sprintf("%d", bodySize))
        request.Header.Add("Content-Type", "application/json")

        resp, err := d.Client.Do(request)
        if(err != nil) {
            errReturn <- err
            return
        }

        defer resp.Body.Close()
        bodyData, err := ioutil.ReadAll(resp.Body)
        if(err != nil) {
            errReturn <- err
            return
        }

        // If there was no response, return nil on the errorChan to indicate no error
        if(len(bodyData) == 0) {
            errReturn <- nil
        } else {
            dataReturn <- bodyData
        }
    }()

    return dataReturn, errReturn
}

func (d *OrientDbDriver) getUrl(method string) string {
    return fmt.Sprintf("http://%s:%d/%s", d.Config.ServerRoot, d.Config.Port, method)
}

func (d *OrientDbDriver) auth(request *http.Request) {
    request.SetBasicAuth(d.Config.Username, d.Config.Password)
}


