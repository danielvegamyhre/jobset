package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	redis "github.com/redis/go-redis/v9"
)

// Environment variable constants
const (
	RedisHostEnv   = "REDIS_HOST"
	RedisPortEnv   = "REDIS_PORT"
	JobsetNameEnv  = "JOBSET_NAME"
	PodNameEnv     = "POD_NAME"
	UserCommandEnv = "USER_COMMAND"
)

// RestartHandler handles process restarts and coordination
type RestartHandler struct {
	mainProcess     *os.Process
	restartsChannel string
	redisLockName   string
	redisClient     *redis.Client
	redisPubSub     *redis.PubSub
	podName         string
	mutex           sync.Mutex
}

// NewRestartHandler creates a new RestartHandler instance
func NewRestartHandler(restartsChannel string) (*RestartHandler, error) {
	redisLockName := os.Getenv(JobsetNameEnv)
	if redisLockName == "" {
		return nil, fmt.Errorf("environment variable %s must be set", JobsetNameEnv)
	}

	redisHost := os.Getenv(RedisHostEnv)
	redisPort := os.Getenv(RedisPortEnv)
	if redisHost == "" || redisPort == "" {
		return nil, fmt.Errorf("environment variables %s and %s must be set", RedisHostEnv, RedisPortEnv)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisHost + ":" + redisPort,
	})

	podName := os.Getenv(PodNameEnv)

	pubsub := redisClient.Subscribe(context.Background(), restartsChannel)
	_, err := pubsub.Receive(context.Background()) // Wait for subscription confirmation
	if err != nil {
		return nil, fmt.Errorf("redis subscription error: %v", err)
	}

	return &RestartHandler{
		restartsChannel: restartsChannel,
		redisLockName:   redisLockName,
		redisClient:     redisClient,
		redisPubSub:     pubsub,
		podName:         podName,
	}, nil
}

func main() {
	_, err := NewRestartHandler("restarts")
	if err != nil {
		panic(err)
	}
}
