package messenger

import (
	"github.com/streadway/amqp"
	"fmt"
	"log"
)

type(
	Connector interface {
		ConsumeQueue(string, func([]byte)[]byte) (error)
		ListQueues() ([]string)
	}
	agent struct{
		conn *amqp.Connection
		chnn *amqp.Channel
		queues []amqp.Queue
		name string
	}
)

func CreateMessenger(name string, username string,password string,server string,port uint32)(Connector, error){
	// establish connection
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/",username,password,server,port))
	if err != nil { return nil, err}
	// create channel
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	defer conn.Close()
	defer ch.Close()
	return &agent{conn,ch,[]amqp.Queue{},name},nil
}

func (a *agent)ConsumeQueue(topic string, callback func(param []byte)([]byte))(error){
	q, err := a.chnn.QueueDeclare(
		topic, // name
		true,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil { return err}

	msgs, err := a.chnn.Consume(
		q.Name, // queue
		a.name,     // consumer
		false,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil { return err}

	go func() {
		for d := range msgs {
			callback(d.Body)
			//log.Printf("Message Acknowledger    : %s", d.Acknowledger)
			//log.Printf("Message Headers         : %s", d.Headers)
			//log.Printf("Message ContentType     : %s", d.ContentType)
			//log.Printf("Message ContentEncoding : %s", d.ContentEncoding)
			//log.Printf("Message DeliveryMode    : %s", d.DeliveryMode)
			//log.Printf("Message Priority        : %s", d.Priority)
			//log.Printf("Message CorrelationId   : %s", d.CorrelationId)
			//log.Printf("Message ReplyTo         : %s", d.ReplyTo)
			//log.Printf("Message Expiration      : %s", d.Expiration)
			//log.Printf("Message MessageId       : %s", d.MessageId)
			//log.Printf("Message Timestamp       : %s", d.Timestamp)
			//log.Printf("Message Type            : %s", d.Type)
			//log.Printf("Message UserId          : %s", d.UserId)
			//log.Printf("Message AppId           : %s", d.AppId)
			//log.Printf("Message ConsumerTag     : %s", d.ConsumerTag)
			//log.Printf("Message MessageCount    : %s", d.MessageCount)
			//log.Printf("Message DeliveryTag     : %s", d.DeliveryTag)
			//log.Printf("Message Redelivered     : %s", d.Redelivered)
			//log.Printf("Message Exchange        : %s", d.Exchange)
			//log.Printf("Message RoutingKey      : %s", d.RoutingKey)
			//log.Printf("Message Body            : %s", d.Body)
			d.Ack(false)
		}
	}()
	log.Printf("Listen to AMQP Topic (%s) ... ... \n",topic)
	a.queues = append(a.queues, q)
	return nil
}

func (a *agent)ListQueues() ([]string){
	result := make([]string, 0)
	for q := range a.queues{
		result = append(result, a.queues[q].Name)
	}
	return result
}