package pubsub

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dchest/uniuri"
	"github.com/frain-dev/cosqs/mongo"
)

type PubSub interface {
	Dispatch()
	Listen()
	Stop()
}

type SafeCounter struct {
	mu    sync.Mutex
	wc    map[string]int
	count int
}

type SQSPubSub struct {
	source mongo.Source
	sfc    *SafeCounter
	done   chan struct{}
}

func newSqsPubSub(source mongo.Source) PubSub {
	return &SQSPubSub{
		source: source,
		sfc:    &SafeCounter{wc: make(map[string]int)},
		done:   make(chan struct{}),
	}
}

func (s *SQSPubSub) Dispatch() {
	for i := 1; i <= s.source.Workers; i++ {
		go s.Listen()
	}
}

func (s *SQSPubSub) cancelled() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *SQSPubSub) Stop() {
	close(s.done)
}

func (s *SQSPubSub) Listen() {
	id := uniuri.NewLen(6)
	fmt.Printf("Running worker with ID: %s\n", id)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s.source.DefaultRegion),
		Credentials: credentials.NewStaticCredentials(s.source.AccessKeyID, s.source.SecretAccessKey, ""),
	})

	if err != nil {
		fmt.Println("error with initializing session", err)
	}

	//create a service client
	svc := sqs.New(sess)
	url, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &s.source.QueueName,
	})

	if err != nil {
		fmt.Println("error with getting queue url,", err)
	}

	//get the queue url
	queueURL := url.QueueUrl
	fmt.Println("queue url is", *queueURL)

	if err != nil {
		fmt.Printf("failed to fetch sqs message %v", err)
	}

	//infinite loop to poll messages from the queue
	for {
		canceled := s.cancelled()
		fmt.Println("canceled is:", canceled)

		if s.cancelled() {
			return
		}

		output, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            queueURL,
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64(1),
		})

		if err != nil {
			fmt.Printf("failed to fetch sqs message %v", err)
		}

		var wg sync.WaitGroup
		for _, message := range output.Messages {
			wg.Add(1)
			go func(m *sqs.Message) {
				defer wg.Done()
				fmt.Println("RECEIVING MESSAGE >>>")
				fmt.Println(*m.Body)
				s.sfc.mu.Lock()
				s.sfc.count++
				fmt.Println("total number of messages received\n", s.sfc.count)
				s.sfc.mu.Unlock()
				fmt.Printf("time :%v", time.Now().Format("Jan _2 15:04:05.000000"))
				svc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      queueURL,
					ReceiptHandle: m.ReceiptHandle,
				})
			}(message)

			wg.Wait()
		}
	}
}

type GooglePubSub struct {
	source mongo.Source
}

func newGooglePubSub(source mongo.Source) PubSub {
	return &GooglePubSub{source: source}
}

func (g *GooglePubSub) Dispatch() {}

func (g *GooglePubSub) Listen() {}

func (g *GooglePubSub) Stop() {}

func NewPubSub(source mongo.Source) (PubSub, error) {
	if source.Type == mongo.SQSPubSub {
		return newSqsPubSub(source), nil
	}

	if source.Type == mongo.GooglePubSub {
		return newGooglePubSub(source), nil
	}

	return nil, fmt.Errorf("source with pub sub type: %s is not supported", source.Type)
}
