package gossdb_client

import (
	"github.com/houbin910902/gossdb_client/gossdb/ssdb"
	"fmt"
)

type DbClient struct {
	Client *ssdb.Client
}

func NewDbClient(ip string, port int, Password string) (*DbClient, error) {
	var db DbClient
	c, err := ssdb.Connect(ip, port)
	if err != nil {
		return &db, err
	}
	db.Client = c
	if Password == ""{
		return &db, nil
	}

	_, err = db.Auth(Password)
	if err == nil{
		return &db, nil
	}
	db.CloseDbClient()
	return nil, fmt.Errorf("auth failed, password is wrong")
}




func (c *DbClient) CloseDbClient() error {
	if c != nil && c.Client != nil{
		return c.Client.Close()
	}
	return nil
}



func (c *DbClient) Auth(Password string) ([]string, error) {
	if Password != "" {
		resp, err := c.Client.Do("auth", []string{Password})
		if err != nil {
			return nil, fmt.Errorf("%s authentication failed", err)
		}
		if len(resp) > 0 && resp[0] == "ok" {
			//验证成功
			return resp, nil
		}
		return resp, fmt.Errorf("auth failed, password is wrong")
	}
	return nil, fmt.Errorf("auth failed")
}