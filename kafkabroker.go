package main

import (
	"strings"
	"third/kafka"
	"backend/common"
	"os"
	"os/signal"
	"sync"
)

type Broker interface {
	GetBrokerList() ([]string, error)
	ConsumeMsg(brokers []string, topic string) error
}

type KafkaBroker struct {
}

func (kafkaBroker *KafkaBroker) GetBrokerList() ([]string, error) {
	brokerList := strings.Split(gBrokers, "|")
	return brokerList, nil
}

func (kafkaBroker *KafkaBroker) ConsumeMsg(brokers []string, topic string) error {
	consumer, err := kafka.NewConsumer(brokers, nil)
	if err != nil {
		common.Logger.Error(err.Error())
		return err
	}
	var (
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		common.Logger.Info("Initiating shutdown of consumer...")
		close(closing)
	}()

	partitionList,err := consumer.Partitions(topic)
	if err != nil {
		common.Logger.Error(err.Error())
		return err
	}

	//针对topic的每一个partition都开启一个partition consumer
	for _, partition := range partitionList {
		common.Logger.Info("Initiating a partition Consumer..")
		partitionConsumer, err := consumer.ConsumePartition(gTopic, partition, kafka.OffsetNewest)
		if err != nil {
			common.Logger.Error(err.Error())
			return err
		}

		wg.Add(1)
		go func(partitionConsumer kafka.PartitionConsumer) {
			defer wg.Done()
			defer partitionConsumer.AsyncClose()
			var channel = make(chan []byte, gChannelBufferSize)
			for i := 0; i < int(gBufferWriterNum); i++ {
				go bufWriter(channel)
			}

			consumed := 0
			ConsumerLoop:
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					common.Logger.Debug("Consumed message %s, offset %d\n", string(msg.Value), msg.Offset)
					channel <- msg.Value
					consumed++
				case <-closing:
					break ConsumerLoop
				}
			}
		}(partitionConsumer)
	}

	wg.Wait()
	common.Logger.Info("Done consuming topic", gTopic)

	if err := consumer.Close(); err != nil {
		common.Logger.Error("Failed to close consumer: ", err)
		return err
	}
	return nil
}
