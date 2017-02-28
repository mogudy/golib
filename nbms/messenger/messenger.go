package messenger

import (
	"github.com/streadway/amqp"
	"fmt"
)

type(
	Connector interface {
		ListenWithFunc(consumerName string,service string,api string,action func([]byte)[]byte) (error)
		SendTo(service string,api string,param string)(error)
		ListQueues() (names []string)
		Close()
	}
	agent struct{
		conn *amqp.Connection
		sndChan *amqp.Channel
		rcvChan *amqp.Channel
		queues []amqp.Queue
	}
)

func CreateMessenger(username string,password string,server string,port uint32)(Connector, error){
	// establish connection
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/",username,password,server,port))
	if err != nil { return nil, err}
	return &agent{conn:conn,queues:[]amqp.Queue{}},nil
}

func (a *agent)ListenWithFunc(consumerName string,exch string, topic string, callback func(param []byte)([]byte))(error){
	// create channel
	if a.rcvChan == nil {
		ch, err := a.conn.Channel()
		if err != nil {
			return err
		}
		a.rcvChan = ch
	}

	err := a.rcvChan.ExchangeDeclare(
		exch, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil { return err}

	q, err := a.rcvChan.QueueDeclare(
		topic, // name
		true,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil { return err}

	err = a.rcvChan.QueueBind(
		topic, // queue name
		topic,     // routing key
		exch, // exchange
		false,
		nil)
	if err != nil { return err}

	msgs, err := a.rcvChan.Consume(
		q.Name, // queue
		consumerName,     // consumer
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
	a.queues = append(a.queues, q)
	return nil
}

func (a *agent)SendTo(exch string, topic string, msg string)(error){
	// create channel
	if a.sndChan == nil {
		ch, err := a.conn.Channel()
		if err != nil {
			return err
		}
		a.sndChan = ch
	}
	err := a.sndChan.Publish(
		exch,     // exchange
		topic, // routing key
		true,  // mandatory
		false,  // immediate
		amqp.Publishing {
			ContentType: "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:        []byte(msg),
		})
	return err
}

func (a *agent)ListQueues() ([]string){
	result := make([]string, 0)
	for q := range a.queues{
		result = append(result, a.queues[q].Name)
	}
	return result
}

func (a *agent)Close() (){
	if a.sndChan!=nil{a.sndChan.Close()}
	if a.rcvChan!=nil{a.rcvChan.Close()}
	if a.conn!=nil{a.conn.Close()}
}