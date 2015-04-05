package gorientdb

import (
    "testing"
    "fmt"
)

var config = OrientConfig{
    "localhost",
    2480,
    "admin",
    "admin",
    "gorient",
}

var driver = NewDriver(config)

func TestGetDatabaseInfo(t *testing.T){
    data, err := driver.DatabaseInfo()
    select {
    case returnedError := <- err:
        fmt.Println(returnedError)
        t.Fail()
    case <- data:
        fmt.Println("Got data!")
    }
}

func TestQuery(t *testing.T) {
    data, err := driver.Command("select @rid from user")
    select {
    case returnedError := <- err:
        t.Error(returnedError)
    case returnedData := <- data:
        fmt.Println(returnedData)
    }
}

