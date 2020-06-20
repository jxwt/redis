package redis

import (
	"fmt"
	"github.com/astaxie/beego/logs"
	"github.com/garyburd/redigo/redis"
)

type Lock struct {
	resource string
	token    string
	conn     redis.Conn
	timeout  int
}

func (lock *Lock) tryLock() (ok bool, err error) {
	_, err = redis.String(lock.conn.Do("SET", lock.key(), lock.token, "EX", int(lock.timeout), "NX"))
	if err == redis.ErrNil {
		// The lock was not successful, it already exists.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (lock *Lock) Unlock() (err error) {
	_, err = lock.conn.Do("del", lock.key())
	lock.conn.Close()
	return err
}

func (lock *Lock) key() string {
	return fmt.Sprint("redislock:%s", lock.resource)
}

func (lock *Lock) AddTimeout(ex_time int64) (ok bool, err error) {
	ttlTime, err := redis.Int64(lock.conn.Do("TTL", lock.key()))
	if err != nil {
		logs.Error("redis get failed:", err)
	}
	if ttlTime > 0 {
		_, err := redis.String(lock.conn.Do("SET", lock.key(), lock.token, "EX", int(ttlTime+ex_time)))
		if err == redis.ErrNil {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func TryLock(resource string, token string, timeOut int) (lock *Lock, ok bool, err error) {
	return TryLockWithTimeout(client.Get(), resource, token, timeOut)
}

func TryLockWithTimeout(conn redis.Conn, resource string, token string, timeout int) (lock *Lock, ok bool, err error) {
	lock = &Lock{resource, token, conn, timeout}
	ok, err = lock.tryLock()
	return lock, ok, err
}

func LockKey(key string, value string, timeout int) (ok bool, err error) {
	conn := client.Get()
	defer conn.Close()
	_, err = redis.String(conn.Do("SET", key, value, "EX", timeout, "NX"))
	if err == redis.ErrNil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
