package gorientdb

import (
	"fmt"
	"testing"
)

var config = OrientConfig{
	"localhost",
	2480,
	"admin",
	"admin",
	"gorient",
}

var driver = NewDriver(config)

func TestGetDatabaseInfo(t *testing.T) {
	data, err := driver.DatabaseInfo()

    var blankTest interface{}
    if data == blankTest || err != nil {
        t.Fail()
    } else {
        fmt.Println(data)
    }
}

func TestQuery(t *testing.T) {
	data, err := driver.Command("select @rid from user")
    if err != nil {
        t.Fail()
    } else {
        fmt.Println(data)
    }
}

func TestCreateClass(t *testing.T) {
	data, err := driver.Command("create class Node extends V")
    if err != nil {
        t.Fail()
    } else {
        fmt.Println(data)
    }
}

func TestClassCount(t *testing.T) {
    count := driver.GetCount("User", false)
    fmt.Println(count)
    if(count < 1) {
        t.Fail()
    }
}
