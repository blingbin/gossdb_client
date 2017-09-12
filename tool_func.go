package gossdb_client

import (
	"fmt"
)

//生成通过的错误信息，已经确定是有错误
func handError(resp []string, paras ...interface{}) error {
	if len(resp) < 1 {
		return fmt.Errorf("ssdb respone error")
	}
	//正常返回的不存在不报错，如果要捕捉这个问题请使用exists
	if resp[0] == "not_found" {
		return nil
	}
	if len(paras) > 0 {
		return fmt.Errorf("access ssdb error, code is %v, parameter is %v", resp, paras)
	} else {
		return fmt.Errorf("access ssdb error, code is %v", resp)
	}
}


