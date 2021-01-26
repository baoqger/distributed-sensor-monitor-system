package coordinator

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/baoqger/distributed-sensor-monitor-system/dto"
	"github.com/baoqger/distributed-sensor-monitor-system/qutils"
	"github.com/streadway/amqp"
)

var url = "amqp://guest:guest@localhost:5672"

type QueueListener struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	sources map[string]<-chan amqp.Delivery // key: sensor name, value: the channel for receiving messages
}

func NewQueueListener() *QueueListener {
	ql := QueueListener{
		sources: make(map[string]<-chan amqp.Delivery),
	}
	ql.conn, ql.ch = qutils.GetChannel(url)
	return &ql
}

// ListenForNewSource func: listen for new sensor's online message
// Each coordinator instance creates a sensor discovery queue and bind to fanout exchange
// Listen for message from this fanout exchange
// For each new sensor, each coordinator subscribe to the data queue with sensor name as routing key created by the sensor itself

func (ql *QueueListener) ListenForNewSource() {
	// leave the queue name as empty, then rabbitmq will create an unique for this queue
	// and return it as the name field in the return value
	q := qutils.GetQueue("", ql.ch)

	// by default, the queue binds to the default direct exchange
	// in this case, the queue created by the coordinator should binds to amq.fanout exchange

	ql.ch.QueueBind(
		q.Name,       // the queue created above
		"",           // key string
		"amq.fanout", // exchange string
		false,        // nowait bool
		nil)

	msgs, _ := ql.ch.Consume(
		q.Name, // queue name, queue is created by coordinator itself
		"",     // comsumer key
		true,
		false,
		false,
		false,
		nil)

	for msg := range msgs {
		sourceChan, _ := ql.ch.Consume(
			string(msg.Body), // msg.Body contains the sensor name
			"",               // comsumer key
			true,
			false,
			false,
			false,
			nil)
		if ql.sources[string(msg.Body)] == nil {
			ql.sources[string(msg.Body)] = sourceChan // register this new sensor

			go ql.AddListener(sourceChan)
		}
	}

}

func (ql *QueueListener) AddListener(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		r := bytes.NewReader(msg.Body)
		d := gob.NewDecoder(r)
		sd := new(dto.SensorMessage)
		d.Decode(sd)

		fmt.Printf("Received message: %v\n", sd)
	}
}
