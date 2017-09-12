package gossdb_client

import (
	"testing"
	"fmt"
)

func TestSet(t *testing.T) {
	db, err:= NewDbClient("127.0.0.1", 8888)
	if err != nil{
		t.Fatalf("NewDbClient fail err: %s", err.Error())
	}

	aRet, err := db.Auth("11111111111111111111111111111111")
	if err != nil{
		t.Fatalf("Auth fail err: %s", err)
	}
	fmt.Println("Auth ret: ", aRet)

	// 开始测试set
	err = db.Set("a", "123456")
	if err != nil{
		t.Fatalf("Set fail. err: %s", err.Error())
	}
	Value, err := db.Get("a")
	if err != nil{
		t.Fatalf("Get fail. err: %s", err.Error())
	}
	if Value != "123456" {
		t.Fatalf("Get fail. Values is %s", Value)
	}
}

func TestAuth(t *testing.T)  {
	db, err := NewDbClient("127.0.0.1", 8888)
	if err != nil{
		t.Fatalf("NewDbClient fail err: %s", err.Error())
	}
	aRet, err := db.Auth("11111111111111111111111111111111")
	if err != nil{
		t.Fatalf("Auth fail err: %s", err)
	}
	fmt.Println("Auth ret: ", aRet)


	// 开始测试set
	err = db.Set("a", "123456")
	if err != nil {
		t.Fatalf("Set fail. err: %s", err.Error())
	}
	mapRet, err := db.MultiGetArray([]string{"a", "b"})
	if err!= nil{
		t.Fatalf("")
	}
	fmt.Println("mapRet: ", mapRet)

}
