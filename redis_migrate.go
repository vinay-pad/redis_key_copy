package main

import (
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis"
)

func newRedisClient(host string, password string) *redis.Client {
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:6379", host),
		Password: password,
	}
	opts.TLSConfig = &tls.Config{ServerName: host}

	return redis.NewClient(opts)
}

func execute(sourceHost string, sourcePassword string, targetHost string, targetPassword string, prefix string) {

	sourceClient := newRedisClient(sourceHost, sourcePassword)
	targetClient := newRedisClient(targetHost, targetPassword)

	stmtKeys := sourceClient.Keys(prefix + "*")

	for _, element := range stmtKeys.Val() {
		val, err := sourceClient.Get(element).Result()
		if err != nil {
			panic(err)
		}
		ttl, err := sourceClient.TTL(element).Result()
		if err != nil {
			panic(err)
		}

		fmt.Printf("Copying key %s, val %s and ttl %d to the target redis\n", element, val, ttl.Nanoseconds())

		targeterr := targetClient.Set(element, val, time.Duration(ttl.Nanoseconds())).Err()
		if targeterr != nil {
			panic(targeterr)
		}
	}

	fmt.Println("Successfully copied over redis key, val and ttls")
}

func main() {

	fmt.Println("Copying over keys, values and ttls from")
	fmt.Println("---------------------")

	if len(os.Args) < 6 {
		panic("Format: go run redis_migrate.go sourceHost sourcePasswd targetHost targetPasswd prefix")
	}

	args := os.Args[1:]

	sourceHost := args[0]
	sourcePasswd := args[1]
	targetHost := args[2]
	targetPasswd := args[3]
	prefix := args[4]

	execute(sourceHost, sourcePasswd, targetHost, targetPasswd, prefix)
}
