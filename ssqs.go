// Package ssqs provides a super simple AWS SQS consumer.
package ssqs

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// Consumer represents a consumer.
type Consumer struct {
	client   sqsiface.SQSAPI
	finish   chan struct{}
	Errors   chan error
	Messages chan *Message
	Queue    *Queue
}

// Message represents a message.
type Message struct {
	Body    string
	ID      string
	Receipt string
}

// Queue represents a queue.
type Queue struct {
	Name              string
	PollDuration      int64
	Region            string
	URL               string
	VisibilityTimeout int64
}

// DeleteError is returned when there's an error deleting a message.
type DeleteError struct {
	Message *Message
}

func (de *DeleteError) Error() string {
	return fmt.Sprintf("error deleting message: %s", de.Message.ID)
}

// New creates and returns a consumer.
func New(q *Queue) *Consumer {
	return &Consumer{
		client:   sqs.New(session.New(), &aws.Config{Region: &q.Region}),
		finish:   make(chan struct{}),
		Errors:   make(chan error),
		Messages: make(chan *Message),
		Queue:    q,
	}
}

// Close closes a consumer.
func (c *Consumer) Close() {
	c.finish <- struct{}{}
}

// Delete deletes a message from the queue.
func (c *Consumer) Delete(m *Message) {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      &c.Queue.URL,
		ReceiptHandle: &m.Receipt,
	}

	if _, err := c.client.DeleteMessage(input); err != nil {
		c.Errors <- &DeleteError{m}
		return
	}
}

// Start starts a consumer.
func (c *Consumer) Start() {
	go c.consume()
}

func (c *Consumer) consume() {
	input := &sqs.ReceiveMessageInput{
		AttributeNames:    []*string{&c.Queue.Name},
		QueueUrl:          &c.Queue.URL,
		VisibilityTimeout: &c.Queue.VisibilityTimeout,
		WaitTimeSeconds:   &c.Queue.PollDuration,
	}

	for {
		select {
		case <-c.finish:
			return
		default:
			c.receive(input)
		}
	}
}

func (c *Consumer) receive(input *sqs.ReceiveMessageInput) {
	r, err := c.client.ReceiveMessage(input)
	if err != nil {
		c.Errors <- err
		return
	}

	for _, v := range r.Messages {
		c.Messages <- &Message{Body: *v.Body, ID: *v.MessageId, Receipt: *v.ReceiptHandle}
	}
}
