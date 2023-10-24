package machinery

import (
	"github.com/RichardKnop/machinery/v2"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
	"github.com/RichardKnop/machinery/v2/config"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
	"github.com/RichardKnop/machinery/v2/tasks"
)

var defaultMapRegisterTask = make(map[string]interface{})

// 配置Server并注册任务
func startServer() (*machinery.Server, error) {
	cnf := &config.Config{
		DefaultQueue:    "machinery_tasks",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	// 创建服务器实例
	broker := redisbroker.NewGR(cnf, []string{"localhost:6379"}, 0)
	backend := redisbackend.NewGR(cnf, []string{"localhost:6379"}, 0)
	lock := eagerlock.New()
	server := machinery.NewServer(cnf, broker, backend, lock)

	// 注册异步任务
	return server, server.RegisterTasks(defaultMapRegisterTask)
}

// Worker 启动Worker
func Worker(consumerTag string) error {
	//消费者的标记
	if len(consumerTag) == 0 {
		consumerTag = "machinery_worker"
	}

	server, err := startServer()
	if err != nil {
		return err
	}

	//第二个参数并发数, 0表示不限制
	worker := server.NewWorker(consumerTag, 0)

	//钩子函数
	errorhandler := func(err error) {}
	pretaskhandler := func(signature *tasks.Signature) {}
	posttaskhandler := func(signature *tasks.Signature) {}

	worker.SetPostTaskHandler(posttaskhandler)
	worker.SetErrorHandler(errorhandler)
	worker.SetPreTaskHandler(pretaskhandler)
	return worker.Launch()
}
