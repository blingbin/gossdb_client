package gossdb_client

import (
	"fmt"
	"github.com/houbin910902/to"
)

//设置 hashmap 中指定 key 对应的值内容.
//
//  setName hashmap 的名字
//  key hashmap 的 key
//  value key 的值
//  返回 err，执行的错误
func (c *DbClient) HSet(setName, key string, value interface{}) (err error) {
	resp, err := c.Client.Do("hset", setName, key, value)
	if err != nil {
		return fmt.Errorf("HSet %s/%s error", setName, key)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, setName, key)
}

//获取 hashmap 中指定 key 的值内容.
//
//  setName hashmap 的名字
//  key hashmap 的 key
//  返回 value key 的值
//  返回 err，执行的错误
func (c *DbClient) HGet(setName, key string) (value string, err error) {
	resp, err := c.Client.Do("hget", setName, key)
	if err != nil {
		return "", fmt.Errorf("HGet %s/%s error", setName, key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.String(resp[1]), nil
	}
	return "", handError(resp, setName, key)
}

//删除 hashmap 中的指定 key，不能通过返回值来判断被删除的 key 是否存在.
//
//  setName hashmap 的名字
//  key hashmap 的 key
//  返回 err，执行的错误
func (c *DbClient) HDel(setName, key string) (err error) {
	resp, err := c.Client.Do("hdel", setName, key)
	if err != nil {
		return fmt.Errorf("HDel %s/%s error", setName, key)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, setName, key)
}

//判断指定的 key 是否存在于 hashmap 中.
//
//  setName hashmap 的名字
//  key hashmap 的 key
//  返回 re，如果当前 key 不存在返回 false
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HExists(setName, key string) (re bool, err error) {
	resp, err := c.Client.Do("hexists", setName, key)
	if err != nil {
		return false, fmt.Errorf("HExists %s/%s error: %s", setName, key, err.Error())
	}

	if len(resp) == 2 && resp[0] == "ok" {
		return resp[1] == "1", nil
	}
	return false, handError(resp, setName, key)
}

//删除 hashmap 中的所有 key
//
//  setName hashmap 的名字
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HClear(setName string) (err error) {
	resp, err := c.Client.Do("hclear", setName)
	if err != nil {
		return fmt.Errorf("HClear %s error", setName)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, setName)
}

//列出 hashmap 中处于区间 (key_start, key_end] 的 key-value 列表. ("", ""] 表示整个区间.
//
//  setName - hashmap 的名字.
//  keyStart - 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd - 返回的结束 key(包含), 空字符串表示 +inf.
//  limit - 最多返回这么多个元素.
//  返回包含 key-value 的关联字典.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HScan(setName string, keyStart, keyEnd string, limit int64, reverse ...bool) (map[string]string, error) {
	cmd := "hscan"
	if len(reverse) > 0 && reverse[0] == true {
		cmd = "hrscan"
	}

	resp, err := c.Client.Do(cmd, setName, keyStart, keyEnd, limit)

	if err != nil {
		return nil, fmt.Errorf("%s %s %s %s %v error", cmd, setName, keyStart, keyEnd, limit)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		re := make(map[string]string)
		size := len(resp)
		for i := 1; i < size-1; i += 2 {
			re[resp[i]] = to.String(resp[i+1])
		}
		return re, nil
	}
	return nil, handError(resp, setName, keyStart, keyEnd, limit)
}

//列出 hashmap 中处于区间 (key_start, key_end] 的 key,value 列表. ("", ""] 表示整个区间.
//
//  setName - hashmap 的名字.
//  keyStart - 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd - 返回的结束 key(包含), 空字符串表示 +inf.
//  limit - 最多返回这么多个元素.
//  返回包含 key-value 的关联字典.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HScanArray(setName string, keyStart, keyEnd string, limit int64, reverse ...bool) ([]string, []string, error) {
	cmd := "hscan"
	if len(reverse) > 0 && reverse[0] == true {
		cmd = "hrscan"
	}
	resp, err := c.Client.Do(cmd, setName, keyStart, keyEnd, limit)

	if err != nil {
		return nil, nil, fmt.Errorf("%s %s %s %s %v error", cmd, setName, keyStart, keyEnd, limit)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		keys := []string{}
		values := []string{}
		size := len(resp)
		for i := 1; i < size-1; i += 2 {
			keys = append(keys, resp[i])
			values = append(values, to.String(resp[i+1]))
		}
		return keys, values, nil
	}
	return nil, nil, handError(resp, setName, keyStart, keyEnd, limit)
}

//列出 hashmap 中处于区间 (key_start, key_end] 的 key,value 列表. ("", ""] 表示整个区间.
//
//  setName - hashmap 的名字.
//  keyStart - 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd - 返回的结束 key(包含), 空字符串表示 +inf.
//  limit - 最多返回这么多个元素.
//  返回包含 key-value 的关联字典.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HRScanArray(setName string, keyStart, keyEnd string, limit int64, reverse ...bool) ([]string, []string, error) {
	return c.HScanArray(setName, keyStart, keyEnd, limit, true)
}

//列出 hashmap 中处于区间 (key_start, key_end] 的 key-value 列表. ("", ""] 表示整个区间.
//
//  setName - hashmap 的名字.
//  keyStart - 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd - 返回的结束 key(包含), 空字符串表示 +inf.
//  limit - 最多返回这么多个元素.
//  返回包含 key-value 的关联字典.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) Hrscan(setName string, keyStart, keyEnd string, limit int64) (map[string]string, error) {
	return c.HScan(setName, keyStart, keyEnd, limit, true)
}

//批量设置 hashmap 中的 key-value.
//
//  setName - hashmap 的名字.
//  kvs - 包含 key-value 的关联数组 .
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHSet(setName string, kvs map[string]interface{}) (err error) {

	args := []interface{}{"multi_hset", setName}
	for k, v := range kvs {
		args = append(args, k)
		args = append(args, v)
	}
	resp, err := c.Client.Do(args...)

	if err != nil {
		return fmt.Errorf("MultiHset %s %s error", setName, kvs)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, setName, kvs)
}

//批量获取 hashmap 中多个 key 对应的权重值.
//
//  setName - hashmap 的名字.
//  keys - 包含 key 的数组 .
//  返回 包含 key-value 的关联数组, 如果某个 key 不存在, 则它不会出现在返回数组中.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHGet(setName string, key ...string) (val map[string]string, err error) {
	if len(key) == 0 {
		return make(map[string]string), nil
	}

	args := []interface{}{"multi_hget", setName}

	for _, v := range key {
		args = append(args, v)
	}

	resp, err := c.Client.Do(args...)
	if err != nil {
		return nil, fmt.Errorf("MultiHget %s %s error", setName, key)
	}
	size := len(resp)
	if size > 0 && resp[0] == "ok" {
		val = make(map[string]string)
		for i := 1; i < size && i+1 < size; i += 2 {
			val[resp[i]] = to.String(resp[i+1])
		}
		return val, nil
	}
	return nil, handError(resp, key)
}

//批量获取 hashmap 中多个 key 对应的权重值.
//
//  setName - hashmap 的名字.
//  keys - 包含 key 的数组 .
//  返回 包含 key和value 的有序数组, 如果某个 key 不存在, 则它不会出现在返回数组中.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHGetSlice(setName string, key ...string) (keys []string, values []string, err error) {
	if len(key) == 0 {
		return []string{}, []string{}, nil
	}
	args := []interface{}{"multi_hget", setName}
	for _, v := range key {
		args = append(args, v)
	}
	resp, err := c.Client.Do(args...)

	if err != nil {
		return nil, nil, fmt.Errorf("MultiHgetSlice %s %s error", setName, key)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		size := len(resp)
		keys := make([]string, 0, (size-1)/2)
		values := make([]string, 0, (size-1)/2)

		for i := 1; i < size && i+1 < size; i += 2 {
			keys = append(keys, resp[i])
			values = append(values, to.String(resp[i+1]))
		}
		return keys, values, nil
	}
	return nil, nil, handError(resp, key)
}

//批量获取 hashmap 中多个 key 对应的权重值.（输入分片）
//
//  setName - hashmap 的名字.
//  keys - 包含 key 的数组 .
//  返回 包含 key-value 的关联数组, 如果某个 key 不存在, 则它不会出现在返回数组中.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHGetArray(setName string, key []string) (val map[string]string, err error) {
	return c.MultiHGet(setName, key...)
}


//批量获取 hashmap 中多个 key 对应的权重值.（输入分片）
//
//  setName - hashmap 的名字.
//  keys - 包含 key 的数组 .
//  返回 包含 key和value 的有序数组, 如果某个 key 不存在, 则它不会出现在返回数组中.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHGetSliceArray(setName string, key []string) (keys []string, values []string, err error) {
	return c.MultiHGetSlice(setName, key...)
}

//批量获取 hashmap 中全部 对应的权重值.
//
//  setName - hashmap 的名字.
//  keys - 包含 key 的数组 .
//  返回 包含 key-value 的关联数组, 如果某个 key 不存在, 则它不会出现在返回数组中.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHGetAll(setName string) (val map[string]string, err error) {

	resp, err := c.Client.Do("hgetall", setName)

	if err != nil {
		return nil, fmt.Errorf("MultiHgetAll %s error", setName)
	}
	size := len(resp)
	if size > 0 && resp[0] == "ok" {
		val = make(map[string]string)
		for i := 1; i < size && i+1 < size; i += 2 {
			val[resp[i]] = to.String(resp[i+1])
		}
		return val, nil
	}
	return nil, handError(resp)
}

//批量获取 hashmap 中全部 对应的权重值.
//
//  setName - hashmap 的名字.
//  返回 包含 key和value 的有序数组, 如果某个 key 不存在, 则它不会出现在返回数组中.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHgetAllSlice(setName string) (keys []string, values []string, err error) {

	resp, err := c.Client.Do("hgetall", setName)

	if err != nil {
		return nil, nil, fmt.Errorf("MultiHgetAllSlice %s error", setName)
	}
	if len(resp) > 0 && resp[0] == "ok" {
		size := len(resp)
		keys := make([]string, 0, (size-1)/2)
		values := make([]string, 0, (size-1)/2)

		for i := 1; i < size && i+1 < size; i += 2 {
			keys = append(keys, resp[i])
			values = append(values, to.String(resp[i+1]))
		}
		return keys, values, nil
	}
	return nil, nil, handError(resp)
}

//批量删除 hashmap 中的 key.
//
//  setName - hashmap 的名字.
//  keys - 包含 key 的数组.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHDel(setName string, key ...string) (err error) {
	if len(key) == 0 {
		return nil
	}
	args := []interface{}{"multi_hdel", setName}
	for _, v := range key {
		args = append(args, v)
	}
	resp, err := c.Client.Do(args...)
	if err != nil {
		return fmt.Errorf("MultiHDel %s %s error", setName, key)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, key)
}

//批量删除 hashmap 中的 key.（输入分片）
//
//  setName - hashmap 的名字.
//  keys - 包含 key 的数组.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) MultiHDelArray(setName string, key []string) (err error) {
	return c.MultiHDel(setName, key...)
}

//列出名字处于区间 (name_start, name_end] 的 hashmap. ("", ""] 表示整个区间.
//
//  nameStart - 返回的起始 key(不包含), 空字符串表示 -inf.
//  nameEnd - 返回的结束 key(包含), 空字符串表示 +inf.
//  limit - 最多返回这么多个元素.
//  返回 包含名字的数组
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HList(nameStart, nameEnd string, limit int64) ([]string, error) {
	resp, err := c.Client.Do("hlist", nameStart, nameEnd, limit)
	if err != nil {
		return nil, fmt.Errorf("HList %s %s %v error", nameStart, nameEnd, limit)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		size := len(resp)
		keyList := make([]string, 0, size-1)

		for i := 1; i < size; i += 1 {
			keyList = append(keyList, resp[i])
		}
		return keyList, nil
	}
	return nil, handError(resp, nameStart, nameEnd, limit)
}

//设置 hashmap 中指定 key 对应的值增加 num. 参数 num 可以为负数.
//
//  setName - hashmap 的名字.
//  key 键值
//  num 增加的值
//  返回 val，整数，增加 num 后的新值
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) HIncR(setName, key string, num int64) (val int64, err error) {

	resp, err := c.Client.Do("hincr", setName, key, num)

	if err != nil {
		return -1, fmt.Errorf("HIncR %s error", key)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.Int64(resp[1]), nil
	}
	return -1, handError(resp, key)
}

//返回 hashmap 中的元素个数.
//
//  setName - hashmap 的名字.
//  返回 val，整数，增加 num 后的新值
//  返回 err，可能的错误，操作成功返回 nil
func (c *DbClient) HSize(setName string) (val int64, err error) {

	resp, err := c.Client.Do("hsize", setName)

	if err != nil {
		return -1, fmt.Errorf("HSize %s error", setName)
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.Int64(resp[1]), nil
	}
	return -1, handError(resp, setName)
}

//列出 hashmap 中处于区间 (keyStart, keyEnd] 的 key 列表.
//
//  name - hashmap 的名字.
//  keyStart - 返回的起始 key(不包含), 空字符串表示 -inf.
//  keyEnd - 返回的结束 key(包含), 空字符串表示 +inf.
//  limit - 最多返回这么多个元素.
//  返回 包含名字的数组
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HKeys(setName, keyStart, keyEnd string, limit int64) ([]string, error) {
	resp, err := c.Client.Do("hkeys", setName, keyStart, keyEnd, limit)
	if err != nil {
		return nil, fmt.Errorf("HKeys %s %s %s %v error", setName, keyStart, keyEnd, limit)
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return resp[1:], nil
	}
	return nil, handError(resp, keyStart, keyEnd, limit)
}

//批量获取 hashmap 中全部 对应的权重值.
//
//  setName - hashmap 的名字.
//  返回 包含 key-value 的关联数组, 如果某个 key 不存在, 则它不会出现在返回数组中.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) HGetAll(setName string) (val map[string]string, err error) {
	return c.MultiHGetAll(setName)
}

