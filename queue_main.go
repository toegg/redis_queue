package redis_queue

import (
	"github.com/go-redis/redis"
	"log"
	"os"
)

type StandaloneQueue struct {
}

func (b StandaloneQueue) Execute(payload *QueuePayload) *QueueResult {

	log.Println("StandaloneQueue:", payload)

	return NewQueueResult(true, "StandaloneQueue.ok", nil)
}

type DemoDemoQueue struct {
}

func (b DemoDemoQueue) Execute(payload *QueuePayload) *QueueResult {

	log.Println("DemoDemoQueue:", payload)
	//测试panic
	if payload.Body == "" {
		panic("empty body")
	}
	return NewQueueResult(true, "DemoDemoQueue.ok", nil)
}

type DemoDemo2Queue struct {
}

func main() {
	// 模拟系统ENV
	os.Setenv("DB_REDIS_BUS_HOST", "127.0.0.1")
	os.Setenv("DB_REDIS_BUS_PORT", "6379")
	os.Setenv("DB_REDIS_BUS_PASSWORD", "")

	host := os.Getenv("DB_REDIS_BUS_HOST")
	port := os.Getenv("DB_REDIS_BUS_PORT")
	password := os.Getenv("DB_REDIS_BUS_PASSWORD")

	// 需要先初始化redis实例
	rds := redis.NewClient(&redis.Options{
		Addr:     host + ":" + port, // remote host
		Password: password,          // password
	})
	// recover通道
	rcLis := make(chan RecoverData, 0)

	// 初始化队列控制器
	qm := NewQueueManager()
	// redis实例到队列控制器
	qm.SetRedis(rds)
	// 实例化recover通道
	qm.SetRecoverLis(rcLis)
	// 注册队列任务处理器，TOPIC::GROUP 方式命名，和入栈队列payload一致
	err := qm.RegisterQueue("DEMO", "DEMO", DemoDemoQueue{})
	check_err(err)
	err = qm.RegisterQueue("DEMO", "DEMO2", DemoDemo2Queue{})
	check_err(err)
	err = qm.RegisterQueue("STANDALONE", "", StandaloneQueue{})
	check_err(err)

	go func() {
	 	// 模拟队列的插入
		qm.QueuePublish(&QueuePayload{
			IsFast: true,
			Topic:  "DEMO",
			Group:  "DEMO",
			Body:   "do the job on channel[DEMO::DEMO]",
		})
		qm.QueuePublish(&QueuePayload{
			IsFast: true,
			Topic:  "DEMO",
			Group:  "DEMO",
			Body:   "",
		})
		qm.QueuePublish(&QueuePayload{
			IsFast: true,
			Topic:  "DEMO",
			Group:  "DEMO",
			Body:   "do the job",
		})
		for i := 0; i< 2; i ++{
			qm.QueuePublish(&QueuePayload{
				IsFast: true,
				Topic:  "DEMO",
				Group:  "DEMO2",
				Body:   "do the job on channel[DEMO]",
			})
			qm.QueuePublish(&QueuePayload{
				IsFast: true,
				Topic:  "STANDALONE",
				Group:  "",
				Body:   "do the job on STANDALONE",
			})

			//time.Sleep(time.Millisecond * 300)
		}
	}()

	for{
		select {
		case rcData := <- rcLis:
			qm.RecoverQueue(rcData)
		}
	}
}

func check_err(err error){
	if err != nil{
		log.Println(err)
	}
}