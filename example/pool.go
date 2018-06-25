package main

import (
	"time"
	"github.com/houbin910902/pool"
	"fmt"
	"github.com/houbin910902/gossdb_client"
)


type SSDBClient struct {
	P pool.Pool
}

var SSDb SSDBClient



func init(){
	//factory 创建连接的方法
	factory := func() (interface{}, error) { return gossdb_client.NewDbClient("127.0.0.1", 8888, "11111111111111111111111111111111") }

	//close 关闭链接的方法
	closeFunc := func(v interface{}) error {  return  v.(*gossdb_client.DbClient).CloseDbClient() }

	//创建一个连接池： 初始化5，最大链接30
	poolConfig := &pool.TPoolConf{
		InitialCap: 5,
		MaxCap:     30,
		Factory:    factory,
		Close:      closeFunc,
		//链接最大空闲时间，超过该时间的链接 将会关闭，可避免空闲时链接EOF，自动失效的问题
		IdleTimeout: 30 * time.Second,
	}
	p, err := pool.NewChannelPool(poolConfig)
	if err != nil {
		fmt.Println("err=", err)
	}
	SSDb.P = p
}

func main()  {
	c, err := SSDb.P.Get()
	if err != nil{
		return
	}
	defer SSDb.P.Put(c) // 每次调用之后应放回连接， 防止资源泄露

	err = c.(*gossdb_client.DbClient).Set("a", "123456")
	if err != nil{
		fmt.Printf("Set fail. err: %s", err.Error())
	}

	Value, err := c.(*gossdb_client.DbClient).Get("a")

	if err != nil{
		fmt.Printf("Get fail. err: %s", err.Error())
	}
	if Value != "123456" {
		fmt.Printf("Get fail. Values is %s", Value)
	}
	fmt.Printf("Value = %v", Value)

}

