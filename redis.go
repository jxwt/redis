package redis

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/logs"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"time"
)

type Params struct {
	RedisAddress 	string
	RedisPassword 	string
	RedisDbIndex 	int
	RedisMaxIdle	int
	RedisMaxTimeOut int
}
var client *redis.Pool

// ErrNil indicates that a reply value is nil.
var ErrNil = errors.New("redigo: nil returned")

func InitRedis(c *Params) {
	client = &redis.Pool{
		MaxIdle:     c.RedisMaxIdle,
		MaxActive:   c.RedisMaxIdle,
		IdleTimeout: time.Duration(c.RedisMaxTimeOut) * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp", c.RedisAddress,
				redis.DialPassword(c.RedisPassword),
				redis.DialDatabase(c.RedisDbIndex),
				redis.DialConnectTimeout(time.Duration(c.RedisMaxTimeOut)*time.Second),
				redis.DialReadTimeout(time.Duration(c.RedisMaxTimeOut)*time.Second),
				redis.DialWriteTimeout(time.Duration(c.RedisMaxTimeOut)*time.Second))
			if err != nil {
				return nil, err
			}
			con.Do("SELECT", c.RedisDbIndex)
			return con, nil
		},
	}
	//redis连接错误判断
	rc := client.Get()
	defer rc.Close()
	if rc.Err() != nil {
		panic(fmt.Sprintf("redis client error:%v", rc.Err()))
	}
}

func Set(key, value string, args ...int64) error {
	conn := client.Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, value)
	if err != nil {
		logs.Error("set error", err.Error())
		return err
	}
	if len(args) > 0 {
		conn.Do("expire", key, args[0])
	}
	return nil
}

// 检查键值是否存在
func Exist(key string) (bool, error) {
	conn := client.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", key))
}

func SetAndExpire(key string, value string, time int64) error {
	conn := client.Get()
	defer conn.Close()
	_, err := redis.String(conn.Do("SET", key, value, "ex", time))
	if err != nil {
		return err
	}
	return nil
}

func Get(key string) (string, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.String(conn.Do("GET", key))
	if err != nil {
		return "", err
	} else {
		return value, nil
	}
}

func GetInt(key string) (int, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.String(conn.Do("GET", key))
	if err != nil {
		return 0, err
	} else {
		return strconv.Atoi(value)
	}
}

func Delete(key string) error {
	conn := client.Get()
	defer conn.Close()
	_, err := redis.String(conn.Do("DEL", key))
	if err != nil {
		return err
	}
	return nil
}

func HGetString(key string, secKey string) (string, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.Strings(conn.Do("hmget", key, secKey))
	if err != nil {
		return "", err
	} else {
		return value[0], nil
	}
}

func HGetInt64(key string, secKey string) (int64, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.Strings(conn.Do("hmget", key, secKey))
	if err != nil {
		return 0, err
	} else {
		return strconv.ParseInt(value[0], 0, 64)
	}
}

// HGetInt .
func HGetInt(key string, field string) (int, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.Int(conn.Do("HGET", key, field))
	if err != nil {
		return 0, err
	}
	return value, nil
}

// HExistsBool .
func HExistsBool(key string, field string) (bool, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.Bool(conn.Do("hexists", key, field))
	if err != nil {
		return value, err
	}
	return value, nil
}

// HSetInterface .
func HSetInterface(key string, field string, value interface{}) error {
	conn := client.Get()
	defer conn.Close()
	if _, err := conn.Do("HSET", key, field, value); err != nil {
		return err
	}
	return nil
}

func HSetMap(key string, val map[string]interface{}, args ...int64) error {
	conn := client.Get()
	defer conn.Close()
	_, err := conn.Do("hmset", redis.Args{}.Add(key).AddFlat(val)...)
	if err != nil {
		logs.Error("set error", err.Error())
		return err
	}
	if len(args) > 0 {
		conn.Do("expire", key, args[0])
	}
	return nil
}

func HSetStruct(key string, val interface{}, args ...int64) error {
	conn := client.Get()
	defer conn.Close()
	_, err := conn.Do("hmset", redis.Args{}.Add(key).AddFlat(val)...)

	if err != nil {
		logs.Error("set error", err.Error())
		return err
	}
	if len(args) > 0 {
		conn.Do("expire", key, args[0])
	}
	return nil
}

func HGetMap(key string) (map[string]string, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.StringMap(conn.Do("hgetall", key))
	if err != nil {
		return nil, err
	} else {
		return value, nil
	}
}

// 哈希表获取value
func HGetValues(key string) ([]interface{}, error) {
	conn := client.Get()
	defer conn.Close()
	value, err := redis.Values(conn.Do("hgetall", key))
	if err != nil {
		return nil, err
	} else {
		return value, nil
	}
}

func SAddMember(key string, member string) error {
	conn := client.Get()
	defer conn.Close()
	_, err := conn.Do("sadd", key, member)
	if err != nil {
		return err
	}
	return nil
}

func SGetMembers(key string) ([]string, error) {
	conn := client.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("SMEMBERS", key))
}

func SRemMember(key string, member string) error {
	conn := client.Get()
	defer conn.Close()
	_, err := conn.Do("SREM", key, member)
	return err
}

func ScanStruct(src []interface{}, dest interface{}) error {
	return redis.ScanStruct(src, dest)
}

// RPush .
func RPush(key string, value string) error {
	conn := client.Get()
	defer conn.Close()
	if _, err := conn.Do("RPUSH", key, value); err != nil {
		return err
	}
	return nil
}

// LPush .
func LPush(key string, value string) error {
	conn := client.Get()
	defer conn.Close()
	if _, err := conn.Do("LPUSH", key, value); err != nil {
		return err
	}
	return nil
}

// LPop .
func LPop(key string) (string, error) {
	conn := client.Get()
	defer conn.Close()
	return redis.String(conn.Do("LPOP", key))
}

// LRange 获取列表起始到结束的值
func LRange(key string, start int, end int) ([]string, error) {
	conn := client.Get()
	defer conn.Close()
	values, err := redis.Strings(conn.Do("LRANGE", key, start, end))
	if err != nil {
		return nil, err
	}
	return values, nil
}

// Expireat 将Key设置unix过期时间
func Expireat(key string, timestamp int) error {
	conn := client.Get()
	defer conn.Close()
	if _, err := conn.Do("EXPIREAT", key, timestamp); err != nil {
		return err
	}
	return nil
}

// HSetInt .
func HDel(key string, field string) error {
	conn := client.Get()
	defer conn.Close()
	if _, err := conn.Do("HDEL", key, field); err != nil {
		return err
	}
	return nil
}

// ZAdd
func ZAdd(key string, score int64, member string) error {
	conn := client.Get()
	defer conn.Close()
	if _, err := conn.Do("ZADD", key, score, member); err != nil {
		return err
	}
	return nil
}

// ZDelete
func ZDelete(key string, member string) error {
	conn := client.Get()
	defer conn.Close()
	if _, err := conn.Do("ZREM", key, member); err != nil {
		return err
	}
	return nil
}

// 根据分数范围查询
func ZRangeByScore(key string, min, max int64) (map[string]string, error) {
	conn := client.Get()
	defer conn.Close()
	ret, err := redis.StringMap(conn.Do("ZRANGEBYSCORE", key, min, max, "WITHSCORES"))

	return ret, err
}

func ZRemRangeByScore(key string, min, max int64) (map[string]string, error) {
	conn := client.Get()
	defer conn.Close()
	ret, err := redis.StringMap(conn.Do("ZREMRANGEBYSCORE", key, min, max))

	return ret, err
}

func GetClient() redis.Conn {
	return client.Get()
}
