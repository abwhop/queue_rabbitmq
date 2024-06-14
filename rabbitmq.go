package queue

import (
	"fmt"
	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	"log"
	"sync"
)

type Queue struct {
	channel *rabbitmq.Channel
}

type Config struct {
	Port     string `env:"RABBITMQ_PORT" env-default:"5672"`
	Host     string `env:"RABBITMQ_HOST" env-default:"localhost"`
	User     string `env:"RABBITMQ_USER" env-default:"user"`
	Password string `env:"RABBITMQ_PASSWORD"`
}

var once sync.Once
var instance *Queue

func GetInstance(cfg Config) (*Queue, error) {
	var err error
	once.Do(func() {
		//fmt.Println(fmt.Sprintf(`amqp://%s:%s@%s:%s`, cfg.User, cfg.Password, cfg.Host, cfg.Port))
		instance, err = newQueue(fmt.Sprintf(`amqp://%s:%s@%s:%s`, cfg.User, cfg.Password, cfg.Host, cfg.Port))
		if err != nil {
			err = fmt.Errorf("error get Queue instance  %w", err)
		}
	})
	return instance, err
}

func newQueue(url string) (*Queue, error) {
	rabbitmq.Debug = true
	conn, err := rabbitmq.Dial(url)
	if err != nil {
		return nil, fmt.Errorf(`error connect to rabbitmq %w`, err)
	}
	consumeCh, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf(`error connect to channel %w`, err)
	}

	err = consumeCh.Qos(1, 0, false)
	if err != nil {
		return nil, fmt.Errorf(`error set qos %w`, err)
	}

	_, err = consumeCh.QueueDeclare("test-auto-delete", false, true, false, true, nil)
	if err != nil {
		return nil, fmt.Errorf(`error get Queue %w`, err)
	}
	return &Queue{channel: consumeCh}, nil
}

func (q *Queue) GetMessage(queueName string, action func(data []byte, contentType string)) {
	go func() {
		d, err := q.channel.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			log.Panic(err)
		}

		for msg := range d {
			action(msg.Body, msg.ContentType)
			if err := msg.Ack(false); err != nil {
				fmt.Println(err)
			}
		}
	}()
}
