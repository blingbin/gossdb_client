package gossdb_client

import (
	"fmt"
	"github.com/houbin910902/to"
)

var (
	qTrimCmd  = []string{"qtrim_front", "qtrim_back"}
	qPushCmd  = []string{"qpush_front", "qpush_back"}
	qPopCmd   = []string{"qpop_front", "qpop_back"}
	qSliceCmd = []string{"qslice", "qrange"}
)

//返回队列的长度.
//
//  name  队列的名字
//  返回 size，队列的长度；
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) Qsize(name string) (size int64, err error) {
	resp, err := c.Client.Do("qsize", name)
	if err != nil {
		return -1, fmt.Errorf("QSize %s error: %s", name, err.Error())
	}

	if len(resp) == 2 && resp[0] == "ok" {
		return to.Int64(resp[1]), nil
	}
	return -1, handError(resp, name)
}

//清空一个队列.
//
//  name  队列的名字
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QClear(name string) (err error) {
	resp, err := c.Client.Do("qclear", name)
	if err != nil {
		return fmt.Errorf("QClear %s error: %s", name, err.Error())
	}

	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, name)
}

//往队列的首部添加一个或者多个元素
//
//  name  队列的名字
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPushFront(name string, value ...interface{}) (size int64, err error) {
	return c.qPush(name, false, value...)
}

//往队列的首部添加一个或者多个元素
//
//  name  队列的名字
//  reverse 是否反向
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) qPush(name string, reverse bool, value ...interface{}) (size int64, err error) {
	if len(value) == 0 {
		return -1, nil
	}
	index := 0
	if reverse {
		index = 1
	}
	args := []interface{}{qPushCmd[index], name}

	args = append(args, value...)

	resp, err := c.Client.Do(args...)
	if err != nil {
		return -1, fmt.Errorf("%s %s error: %s", qPushCmd[index], name, err.Error())
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.Int64(resp[1]), nil
	}
	return -1, handError(resp, name)
}

//往队列的尾部添加一个或者多个元素
//
//  name  队列的名字
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPush(name string, value ...interface{}) (size int64, err error) {
	return c.qPush(name, true, value...)
}

//往队列的尾部添加一个或者多个元素
//
//  name  队列的名字
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPushBack(name string, value ...interface{}) (size int64, err error) {
	return c.qPush(name, true, value...)
}

//从队列首部弹出最后一个元素.
//
//  name 队列的名字
//  返回 v，返回一个元素，并在队列中删除 v；队列为空时返回空值
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPopFront(name string) (v string, err error) {
	return c.QPop(name)
}

//从队列尾部弹出最后一个元素.
//
//  name 队列的名字
//  返回 v，返回一个元素，并在队列中删除 v；队列为空时返回空值
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPopBack(name string) (v string, err error) {
	return c.QPop(name, true)
}

//从队列首部弹出最后一个元素.
//
//  name 队列的名字
//  返回 v，返回一个元素，并在队列中删除 v；队列为空时返回空值
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPop(name string, reverse ...bool) (v string, err error) {
	index := 0
	if len(reverse) > 0 && !reverse[0] {
		index = 1
	}
	resp, err := c.Client.Do(qPopCmd[index], name)
	if err != nil {
		return "", fmt.Errorf("%s %s error: %s", qPopCmd[index], name, err.Error())
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.String(resp[1]), nil
	}
	return "", handError(resp, name)
}

//从队列首部弹出最后多个元素.
//
//  name 队列的名字
//  返回 v，返回多个元素，并在队列中弹出多个元素；
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPopFrontArray(name string, size int64) (v []string, err error) {
	return c.QPopArray(name, size, false)
}

//从队列尾部弹出最后多个元素.
//
//  name 队列的名字
//  返回 v，返回多个元素，并在队列中弹出多个元素；
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPopBackArray(name string, size int64) (v []string, err error) {
	return c.QPopArray(name, size, true)
}

//从队列首部弹出最后多个个元素.
//
//  name 队列的名字
//  size 取出元素的数量
//  reverse 是否反转取
//  返回 v，返回多个元素，并在队列中弹出多个元素；
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPopArray(name string, size int64, reverse ...bool) (v []string, err error) {
	index := 1
	if len(reverse) > 0 && !reverse[0] {
		index = 0
	}
	resp, err := c.Client.Do(qPopCmd[index], name, size)
	if err != nil {
		return nil, fmt.Errorf("%s %s error: %s", qPopCmd[index], name, err.Error())
	}

	respsize := len(resp)
	if respsize > 0 && resp[0] == "ok" {
		for i := 1; i < respsize; i++ {
			v = append(v, to.String(resp[i]))
		}
		return
	}
	return nil, handError(resp, name)
}

//返回下标处于区域 [offset, offset + limit] 的元素.
//
//  name queue 的名字.
//  offset 整数, 从此下标处开始返回. 从 0 开始. 可以是负数, 表示从末尾算起.
//  limit 正整数, 最多返回这么多个元素.
//  返回 v，返回元素的数组，为空时返回 nil
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QRange(name string, offset, limit int) (v []string, err error) {
	return c.slice(name, offset, limit, 1)
}

//返回下标处于区域 [begin, end] 的元素. begin 和 end 可以是负数
//
//  name queue 的名字.
//  begin 正整数, 从此下标处开始返回。从 0 开始。
//  end 整数, 结束下标。可以是负数, 表示返回所有。
//  返回 v，返回元素的数组，为空时返回 nil
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QSlice(name string, begin, end int) (v []string, err error) {
	return c.slice(name, begin, end, 0)
}

//返回下标处于区域 [begin, end] 的元素. begin 和 end 可以是负数
//
//  name queue 的名字.
//  begin 正整数, 从此下标处开始返回。从 0 开始。
//  end 整数, 结束下标。可以是负数, 表示返回所有。
//  [slice，range] 命令
//  返回 v，返回元素的数组，为空时返回 nil
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) slice(name string, args ...int) (v []string, err error) {
	begin := 0
	end := -1
	index := 0
	if len(args) > 0 {
		begin = args[0]
	}
	if len(args) > 1 {
		end = args[1]
	}

	if len(args) > 2 {
		index = args[2]
	}
	resp, err := c.Client.Do(qSliceCmd[index], name, begin, end)
	if err != nil {
		return nil, fmt.Errorf("%s %s error: %s", qSliceCmd[index], name, err.Error())
	}
	size := len(resp)
	if size >= 1 && resp[0] == "ok" {
		for i := 1; i < size; i++ {
			v = append(v, to.String(resp[i]))
		}
		return
	}
	return nil, handError(resp, name)
}

//从队列头部删除多个元素.
//
//  name queue 的名字.
//  size 最多从队列删除这么多个元素
//  reverse 可选，是否反向执行
//  返回 delSize，返回被删除的元素数量
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QTrim(name string, size int, reverse ...bool) (delSize int64, err error) {
	index := 0
	if len(reverse) > 0 && reverse[0] {
		index = 1
	}
	resp, err := c.Client.Do(qTrimCmd[index], name, size)
	if err != nil {
		return -1, fmt.Errorf("%s %s error: %s", qTrimCmd[index], name, err.Error())
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.Int64(resp[1]), nil
	}
	return -1, handError(resp, name)
}

//从队列头部删除多个元素.
//
//  name queue 的名字.
//  size 最多从队列删除这么多个元素
//  返回 v，返回元素的数组，为空时返回 nil
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QTrimFront(name string, size int) (delSize int64, err error) {
	return c.QTrim(name, size)
}

//从队列尾部删除多个元素.
//
//  name queue 的名字.
//  size 最多从队列删除这么多个元素
//  返回 v，返回元素的数组，为空时返回 nil
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QTrimBack(name string, size int) (delSize int64, err error) {
	return c.QTrim(name, size, true)
}

//列出名字处于区间 (name_start, name_end] 的 queue/list.
//
//  name_start  返回的起始名字(不包含), 空字符串表示 -inf.
//  name_end  返回的结束名字(包含), 空字符串表示 +inf.
//  limit  最多返回这么多个元素.
//  返回 v，返回元素的数组，为空时返回 nil
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QList(nameStart, nameEnd string, limit int64) ([]string, error) {
	resp, err := c.Client.Do("qlist", nameStart, nameEnd, limit)
	if err != nil {
		return nil, fmt.Errorf("QList %s %s %v error: %s", nameStart, nameEnd, limit, err.Error())
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

//列出名字处于区间 (name_start, name_end] 的 queue/list.
//
//  name_start  返回的起始名字(不包含), 空字符串表示 -inf.
//  name_end  返回的结束名字(包含), 空字符串表示 +inf.
//  limit  最多返回这么多个元素.
//  返回 v，返回元素的数组，为空时返回 nil
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QRList(nameStart, nameEnd string, limit int64) ([]string, error) {
	resp, err := c.Client.Do("qrlist", nameStart, nameEnd, limit)
	if err != nil {
		return nil, fmt.Errorf("QRList %s %s %v error: %s", nameStart, nameEnd, limit, err.Error())
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

//更新位于 index 位置的元素. 如果超过现有的元素范围, 会返回错误.
//
//  key  队列的名字
//  index 指定的位置，可传负数.
//  val  传入的值.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QSet(key string, index int64, val interface{}) (err error) {
	var resp []string

	resp, err = c.Client.Do("qset", key, index, val)

	if err != nil {
		return fmt.Errorf("QSet %s error: %s", key, err.Error())
	}
	if len(resp) > 0 && resp[0] == "ok" {
		return nil
	}
	return handError(resp, key)
}

//返回指定位置的元素. 0 表示第一个元素, 1 是第二个 ... -1 是最后一个.
//
//  key  队列的名字
//  index 指定的位置，可传负数.
//  返回 val，返回的值.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QGet(key string, index int64) (string, error) {
	resp, err := c.Client.Do("qget", key, index)
	if err != nil {
		return "", fmt.Errorf("QGet %s error: %s", key, err.Error())
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.String(resp[1]), nil
	}
	return "", handError(resp, key)
}

//返回队列的第一个元素.
//
//  key  队列的名字
//  返回 val，返回的值.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QFront(key string) (string, error) {
	resp, err := c.Client.Do("qfront", key)
	if err != nil {
		return "", fmt.Errorf("QFront %s error: %s", key, err.Error())
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.String(resp[1]), nil
	}
	return "", handError(resp, key)
}

//返回队列的最后一个元素.
//
//  key  队列的名字
//  返回 val，返回的值.
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QBack(key string) (string, error) {
	resp, err := c.Client.Do("qback", key)
	if err != nil {
		return "", fmt.Errorf("QBack %s error: %s", key, err.Error())
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.String(resp[1]), nil
	}
	return "", handError(resp, key)
}

//往队列的首部添加一个或者多个元素
//
//  name  队列的名字
//  reverse 是否反向
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) qPushArray(name string, reverse bool, value []interface{}) (size int64, err error) {
	if len(value) == 0 {
		return -1, nil
	}
	index := 0
	if reverse {
		index = 1
	}
	args := []interface{}{qPushCmd[index], name}
	args = append(args, value...)
	resp, err := c.Client.Do(args...)
	if err != nil {
		return -1, fmt.Errorf("%s %s error: %s", qPushCmd[index], name, err.Error())
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return to.Int64(resp[1]), nil
	}
	return -1, handError(resp, name)
}

//往队列的尾部添加一个或者多个元素
//
//  name  队列的名字
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPushArray(name string, value []interface{}) (size int64, err error) {
	return c.qPushArray(name, true, value)
}

//往队列的尾部添加一个或者多个元素
//
//  name  队列的名字
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPushBackArray(name string, value []interface{}) (size int64, err error) {
	return c.qPushArray(name, true, value)
}

//往队列的首部添加一个或者多个元素
//
//  name  队列的名字
//  value  存贮的值，可以为多值.
//  返回 size，添加元素之后, 队列的长度
//  返回 err，执行的错误，操作成功返回 nil
func (c *DbClient) QPushFrontArray(name string, value []interface{}) (size int64, err error) {
	return c.qPushArray(name, false, value)
}
