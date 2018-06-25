package gossdb_client

import (
	"fmt"
	"strconv"
	"github.com/houbin910902/to"
)

//  设置指定 key 的值内容
//  key 键值
//  val 存贮的value值, val只支持基本的类型, 如果要支持复杂的类型, 需要开启连接池的Encoding选项
//  ttl 可选, 设置的过期时间, 单位为秒
//  返回err, 可能的错误, 操作成功返回nil
func (c *DbClient) Set(key string, val interface{}, ttl ...int64) (err error) {
	var resp []string
	if len(ttl) > 0 {
		resp, err = c.Client.Do("setx", key, val, ttl[0])
	} else {
		resp, err = c.Client.Do("set", key, val)
	}
	if err != nil {
		return fmt.Errorf("%s Set %s error", err.Error(), key)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, key)
}

//  当key不存在时, 设置指定key的值内容. 如果已存在, 则不设置.
//  key 键值
//  val 存贮的value值, val只支持基本的类型, 如果要支持复杂的类型, 需要开启连接池的Encoding选项
//  返回 err, 可能的错误, 操作成功返回nil
//  返回 val 1: value 已经设置, 0: key 已经存在, 不更新.
func (c *DbClient) SetNx(key string, val interface{}) (string, error) {
	resp, err := c.Client.Do("setnx", key, val)

	if err != nil {
		return "", fmt.Errorf("%s SetNx %s error", err.Error(), key)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return string(resp[1]), nil
	}
	return "", handError(resp, key)
}

//  获取指定key的值内容
//  key 键值
//  返回 一个 Value,可以方便的向其它类型转换
//  返回 一个可能的错误，操作成功返回 nil
func (c *DbClient) Get(key string) (string, error) {
	resp, err := c.Client.Do("get", key)
	if err != nil {
		return "", fmt.Errorf("%s Get %s error", err.Error(), key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return string(resp[1]), nil
	}
	return "", handError(resp, key)
}


//  更新key对应的value, 并返回更新前的旧的 value.
//  key 键值
//  val 存贮的value值, val只支持基本的类型, 如果要支持复杂的类型, 需要开启连接池的Encoding选项
//  返回 一个 Value, 可以方便的向其它类型转换. 如果key不存在则返回"", 否则返回key对应的值内容.
//  返回 一个可能的错误，操作成功返回 nil
func (c *DbClient) GetSet(key string, val interface{}) (string, error) {
	resp, err := c.Client.Do("getset", key, val)
	if err != nil {
		return "", fmt.Errorf("%s Getset %s error", err.Error(), key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return string(resp[1]), nil
	}
	return "", handError(resp, key)
}

//  设置过期
//  key 要设置过期的 key
//  ttl 存活时间(秒)
//  返回 re, 设置是否成功，如果当前 key 不存在返回 false
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) Expire(key string, ttl int64) (re bool, err error) {
	resp, err := c.Client.Do("expire", key, ttl)
	if err != nil {
		return false, fmt.Errorf("%s Expire %s error", err.Error(), key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1] == "1", nil
	}
	return false, handError(resp, key, ttl)
}

//  查询指定 key 是否存在
//  key 要查询的 key
//  返回 re，如果当前 key 不存在返回 false
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) Exists(key string) (re bool, err error) {
	resp, err := c.Client.Do("exists", key)
	if err != nil {
		return false, fmt.Errorf("%s Exists %s error", err.Error(), key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1] == "1", nil
	}
	return false, handError(resp, key)
}

//  删除指定 key
//  key 要删除的 key
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) Del(key string) error {
	resp, err := c.Client.Do("del", key)
	if err != nil {
		return fmt.Errorf("%s Del %s error", err.Error(), key)
	}
	//response looks like s: [ok 1]
	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, key)
}


//  返回 key(只针对 KV 类型) 的存活时间.
//  key 要删除的 key
//  返回 ttl, key 的存活时间(秒), -1 表示没有设置存活时间.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) Ttl(key string) (ttl int64, err error) {
	resp, err := c.Client.Do("ttl", key)
	if err != nil {
		return -1, fmt.Errorf("%s Ttl %s error", err.Error(), key)
	}
	//response looks like s: [ok 1]
	if len(resp) > 0 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return -1, handError(resp, key)
}

//  使key对应的值增加num. 参数num可以为负数.
//  key 键值
//  num 增加的值
//  返回 val，整数，增加 num 后的新值
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) Inc(key string, num int64) (val int64, err error) {

	resp, err := c.Client.Do("incr", key, num)

	if err != nil {
		return -1, fmt.Errorf("%s Incr %s error", err.Error(), key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return -1, handError(resp, key)
}

//  批量设置一批 key-value.
//  包含 key-value 的字典
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) MultiSet(kvs map[string]interface{}) (err error) {

	args := []interface{}{}

	for k, v := range kvs {
		args = append(args, k)
		args = append(args, v)
	}
	resp, err := c.Client.Do("multi_set", args)

	if err != nil {
		return fmt.Errorf("%s MultiSet %s error", err.Error(), kvs)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, kvs)
}

//  批量获取一批 key 对应的值内容.
//  key, 要获取的 key，可以为多个
//  返回 val, 一个包含返回的 map
//  返回 err, 可能的错误, 操作成功返回 nil
func (c *DbClient) MultiGet(key ...string) (val map[string]string, err error) {
	if len(key) == 0 {
		return make(map[string]string), nil
	}
	resp, err := c.Client.Do("multi_get", key)

	if err != nil {
		return nil, fmt.Errorf("%s MultiGet %s error", err.Error(), key)
	}

	size := len(resp)
	if size > 0 && resp[0] == "ok" {
		val = make(map[string]string)
		for i := 1; i < size && i+1 < size; i += 2 {
			val[resp[i]] = resp[i+1]
		}
		return val, nil
	}
	return nil, handError(resp, key)
}

//  批量获取一批 key 对应的值内容.
//  key, 要获取的 key，可以为多个
//  返回 keys和value分片
//  返回 err, 可能的错误, 操作成功返回 nil
func (c *DbClient) MultiGetSlice(key ...string) (keys []string, values []string, err error) {
	if len(key) == 0 {
		return []string{}, []string{}, nil
	}
	resp, err := c.Client.Do("multi_get", key)

	if err != nil {
		return nil, nil, fmt.Errorf("%s MultiGet %s error", err, key)
	}

	size := len(resp)
	if size > 0 && resp[0] == "ok" {

		keys := make([]string, 0, (size-1)/2)
		values := make([]string, 0, (size-1)/2)

		for i := 1; i < size && i+1 < size; i += 2 {
			keys = append(keys, resp[i])
			values = append(values, resp[i+1])
		}
		return keys, values, nil
	}
	return nil, nil, handError(resp, key)
}

//  批量获取一批 key 对应的值内容.（输入分片）
//  key, 要获取的 key, 可以为多个
//  返回 val, 一个包含返回的 map
//  返回 err, 可能的错误, 操作成功返回 nil
func (c *DbClient) MultiGetArray(key []string) (val map[string]string, err error) {
	if len(key) == 0 {
		return make(map[string]string), nil
	}
	resp, err := c.Client.Do("multi_get", key)

	if err != nil {
		return nil, fmt.Errorf("%s MultiGet %s error", err, key)
	}

	size := len(resp)
	if size > 0 && resp[0] == "ok" {
		val = make(map[string]string)
		for i := 1; i < size && i+1 < size; i += 2 {
			val[resp[i]] = resp[i+1]
		}
		return val, nil
	}
	return nil, handError(resp, key)
}

//  批量获取一批 key 对应的值内容.（输入分片）
//  key, 要获取的 key, 可以为多个
//  返回 keys和value分片
//  返回 err, 可能的错误，操作成功返回 nil
func (c *DbClient) MultiGetArraySlice(key []string) (keys []string, values []string, err error) {
	if len(key) == 0 {
		return []string{}, []string{}, nil
	}
	resp, err := c.Client.Do("multi_get", key)

	if err != nil {
		return nil, nil, fmt.Errorf("%s MultiGet %s error", err, key)
	}

	size := len(resp)
	if size > 0 && resp[0] == "ok" {

		keys := make([]string, 0, (size-1)/2)
		values := make([]string, 0, (size-1)/2)

		for i := 1; i < size && i+1 < size; i += 2 {
			keys = append(keys, resp[i])
			values = append(values, resp[i+1])
		}
		return keys, values, nil
	}
	return nil, nil, handError(resp, key)
}

//批量删除一批 key 和其对应的值内容.
//
//  key，要删除的 key，可以为多个
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) MultiDel(key ...string) (err error) {
	if len(key) == 0 {
		return nil
	}
	resp, err := c.Client.Do("multi_del", key)

	if err != nil {
		return fmt.Errorf("%s MultiDel %s error", err, key)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, key)
}

//  设置字符串内指定位置的位值(BIT), 字符串的长度会自动扩展.
//  key 键值
//  offset 位偏移
//  bit  0 或 1
//  返回 val, 原来的位值
//  返回 err, 可能的错误, 操作成功返回 nil
func (c *DbClient) SetBit(key string, offset int64, bit byte) (byte, error) {

	resp, err := c.Client.Do("setbit", key, offset, bit)

	if err != nil {
		return 255, fmt.Errorf("%s SetBit %s error", err, key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return byte(int8(to.Int64(resp[1]))), nil
	}
	return 255, handError(resp, key)
}

//获取字符串内指定位置的位值(BIT).
//
//  key 键值
//  offset 位偏移
//  返回 val，位值
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) GetBit(key string, offset int64) (byte, error) {
	resp, err := c.Client.Do("getbit", key, offset)
	if err != nil {
		return 255, fmt.Errorf("%s GetBit %s error", err, key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return byte(int8(to.Int64(resp[1]))), nil
	}
	return 255, handError(resp, key)
}


//  获取字符串的子串.
//  key 键值
//  start int, 子串的字节偏移;若 start 是负数, 则从字符串末尾算起.
//  size  int,可选, 子串的长度(字节数), 默认为到字符串最后一个字节; 若 size 是负数, 则表示从字符串末尾算起, 忽略掉那么多字节(类似 PHP 的 substr())
//  返回 val, 字符串的部分
//  返回 err, 可能的错误，操作成功返回 nil
func (c *DbClient) Substr(key string, start int64, size ...int64) (val string, err error) {
	var resp []string
	if len(size) > 0 {
		resp, err = c.Client.Do("substr", key, start, size[0])
	} else {
		resp, err = c.Client.Do("substr", key, start)
	}

	if err != nil {
		return "", fmt.Errorf("%s Substr %s error", err, key)
	}
	if len(resp) > 1 && resp[0] == "ok" {
		return resp[1], nil
	}
	return "", handError(resp, key)
}

//计算字符串的长度(字节数).
//
//  key 键值
//  返回 字符串的长度, key 不存在则返回 0.
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) StrLen(key string) (int64, error) {
	resp, err := c.Client.Do("strlen", key)
	if err != nil {
		return -1, fmt.Errorf("%s Strlen %s error", err, key)
	}
	if len(resp) > 1 && resp[0] == "ok" {
		return strconv.ParseInt(resp[1], 10, 64)
	}
	return -1, handError(resp, key)
}

//  列出处于区间 (key_start, key_end] 的 key 列表.("", ""] 表示整个区间.
//  keyStart int 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd int 返回的结束 key(包含), 空字符串表示 +inf.
//  limit int 最多返回这么多个元素.
//  返回 返回包含 key 的数组.
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) Keys(keyStart, keyEnd string, limit int64) ([]string, error) {

	resp, err := c.Client.Do("keys", keyStart, keyEnd, limit)

	if err != nil {
		return nil, fmt.Errorf("%s Keys %s %s %s error", err, keyStart, keyEnd, limit)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return resp[1:], nil
	}
	return nil, handError(resp, keyStart, keyEnd, limit)
}

//列出处于区间 (key_start, key_end] 的 key 列表.("", ""] 表示整个区间.反向选择
//
//  keyStart int 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd int 返回的结束 key(包含), 空字符串表示 +inf.
//  limit int 最多返回这么多个元素.
//  返回 返回包含 key 的数组.
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) RKeys(keyStart, keyEnd string, limit int64) ([]string, error) {
	resp, err := c.Client.Do("rkeys", keyStart, keyEnd, limit)
	if err != nil {
		return nil, fmt.Errorf("%s Rkeys %s %s %s error", err, keyStart, keyEnd, limit)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return resp[1:], nil
	}
	return nil, handError(resp, keyStart, keyEnd, limit)
}

//  列出处于区间 (key_start, key_end] 的 key-value 列表.("", ""] 表示整个区间.
//  keyStart int 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd int 返回的结束 key(包含), 空字符串表示 +inf.
//  limit int 最多返回这么多个元素.
//  返回 返回包含 key 的数组.
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) Scan(keyStart, keyEnd string, limit int64) (map[string]string, error) {

	resp, err := c.Client.Do("scan", keyStart, keyEnd, limit)

	if err != nil {
		return nil, fmt.Errorf("%s Scan %s %s %s error", err, keyStart, keyEnd, limit)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		re := make(map[string]string)
		size := len(resp)
		for i := 1; i < size-1; i += 2 {
			re[resp[i]] = resp[i+1]
		}
		return re, nil
	}
	return nil, handError(resp, keyStart, keyEnd, limit)
}



//  列出处于区间 (key_start, key_end] 的 key-value 列表, 反向顺序.("", ""] 表示整个区间.
//  keyStart int 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd int 返回的结束 key(包含), 空字符串表示 +inf.
//  limit int 最多返回这么多个元素.
//  返回 返回包含 key 的数组.
//  返回 err, 可能的错误, 操作成功返回 nil
func (c *DbClient) RScan(keyStart, keyEnd string, limit int64) (map[string]string, error) {

	resp, err := c.Client.Do("rscan", keyStart, keyEnd, limit)

	if err != nil {
		return nil, fmt.Errorf("%s Rscan %s %s %s error", err, keyStart, keyEnd, limit)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		re := make(map[string]string)
		size := len(resp)
		for i := 1; i < size-1; i += 2 {
			re[resp[i]] = resp[i+1]
		}
		return re, nil
	}
	return nil, handError(resp, keyStart, keyEnd, limit)
}
