package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MiniRedis struct {
	mu   sync.RWMutex
	data map[string]valueWithExpiry
}

type valueWithExpiry struct {
	value  string
	expiry time.Time
}

func NewMiniRedis() *MiniRedis {
	mr := &MiniRedis{
		data: make(map[string]valueWithExpiry),
	}
	go mr.cleanupExpiredKeys(time.Second * 3)
	return mr
}

func (m *MiniRedis) Set(key, value string, expiresDuration *time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var expiry time.Time
	if expiresDuration != nil && expiresDuration.Seconds() > 0 {
		expiry = time.Now().Add(*expiresDuration)
	}
	m.data[key] = valueWithExpiry{
		value:  value,
		expiry: expiry,
	}
}

func (m *MiniRedis) Get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return "", false
	}
	if v.expiry.IsZero() || v.expiry.After(time.Now()) {
		return v.value, true
	} else {
		delete(m.data, key)
	}
	return "", false
}

func (m *MiniRedis) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func (m *MiniRedis) TTL(key string) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return -2, false
	}
	if v.expiry.IsZero() {
		return -1, true
	}
	if v.expiry.After(time.Now()) {
		return int64(v.expiry.Sub(time.Now()).Seconds()), true
	}
	delete(m.data, key)
	return -2, false
}

func (m *MiniRedis) cleanupExpiredKeys(interval time.Duration) {
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			now := time.Now()
			for k, v := range m.data {
				if !v.expiry.IsZero() && v.expiry.Before(now) {
					delete(m.data, k)
				}
			}
			m.mu.Unlock()
		}
	}
}

func main() {
	mr := NewMiniRedis()

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)

	log.Println("Server is running on port 6379")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}
		go handleRequest(conn, mr)
	}
}

func handleRequest(conn net.Conn, mr *MiniRedis) {
	defer func(conn net.Conn) {
		_ = conn.Close()
	}(conn)

	reader := bufio.NewReader(conn)
	for {
		cmdLine, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Error reading command: ", err)
			return
		}
		cmdLine = strings.TrimSpace(cmdLine)
		cmdParts := strings.Fields(cmdLine)
		action := strings.ToUpper(cmdParts[0])
		log.Println("cmd: ", cmdParts)
		switch action {
		case "SET":
			if len(cmdParts) < 3 {
				_, _ = conn.Write([]byte("ERR wrong number of arguments for 'SET' command\n"))
				continue
			}
			var expiresDuration *time.Duration
			if len(cmdParts) == 5 && strings.ToUpper(cmdParts[3]) == "EX" {
				seconds := cmdParts[4]
				duration, err := time.ParseDuration(seconds + "s")
				if err != nil {
					_, _ = conn.Write([]byte("ERR invalid expire time\n"))
					continue
				}
				expiresDuration = &duration
			}
			mr.Set(cmdParts[1], cmdParts[2], expiresDuration)
			_, _ = conn.Write([]byte("OK\n"))
		case "GET":
			if len(cmdParts) != 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\n"))
				continue
			}
			value, ok := mr.Get(cmdParts[1])
			if !ok {
				_, _ = conn.Write([]byte("$-1\n"))
				continue
			}
			log.Println("value: ", value)
			_, _ = conn.Write([]byte(fmt.Sprintf("$%s\n", value)))
		case "DEL":
			if len(cmdParts) != 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'DEL' command\n"))
				continue
			}
			mr.Delete(cmdParts[1])
			_, _ = conn.Write([]byte("OK\n"))
		case "TTL":
			if len(cmdParts) != 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'TTL' command\n"))
				continue
			}
			ttl, ok := mr.TTL(cmdParts[1])
			if !ok {
				_, _ = conn.Write([]byte("-2\n"))
				continue
			}
			_, _ = conn.Write([]byte(strconv.FormatInt(ttl, 10) + "\n"))
		default:
			_, _ = conn.Write([]byte("-ERR unknown command\n"))
		}
	}

}
