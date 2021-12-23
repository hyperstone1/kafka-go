package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/hyperstone1/go-kafka/repository"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default("important").String()
	messageCountStart int
)

func main() {
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := *brokerList

	master, err := sarama.NewConsumer(brokers, config)

	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Panic(err)
	}

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, os.Interrupt)

	forever := make(chan struct{})

	rep := repository.New()
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)

			case msg := <-consumer.Messages():
				messageCountStart++
				log.Println("Received messages :", string(msg.Key), string(msg.Value))
				rep.Record(string(msg.Key), string(msg.Value))
			case <-signals:
				log.Println("Interrupt is detected")
				forever <- struct{}{}
			}

		}
	}()
	<-forever
	log.Println("Processed", messageCountStart, "messages")

}
