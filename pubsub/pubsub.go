package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dchest/uniuri"
	"github.com/frain-dev/cosqs/mongo"

	"cloud.google.com/go/pubsub"
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
				fmt.Println("RECEIVING MESSAGE FROM SQS")
				fmt.Printf("message received: %s\n", *m.Body)
				s.sfc.mu.Lock()
				s.sfc.count++
				fmt.Printf("total number of messages received: %v\n", s.sfc.count)
				s.sfc.mu.Unlock()
				fmt.Printf("time\n :%v", time.Now().Format("15:04:05\n"))
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
	cancel context.CancelFunc
	sfc    *SafeCounter
	ctx    context.Context
}

func newGooglePubSub(source mongo.Source) PubSub {
	ctx, cancel := context.WithCancel(context.Background())

	return &GooglePubSub{
		source: source,
		ctx:    ctx,
		sfc:    &SafeCounter{wc: make(map[string]int)},
		cancel: cancel,
	}
}

func (g *GooglePubSub) Dispatch() {
	go g.Listen()
}

func (g *GooglePubSub) Stop() {
	g.cancel()
}

func (g *GooglePubSub) Listen() {
	client, err := pubsub.NewClient(context.Background(), g.source.ProjectID)

	if err != nil {
		fmt.Println("error with setting up client", err)
	}

	defer client.Close()

	sub := client.Subscription("convoy-pub-sub-sub")
	// We might need to save the sub.ID here within the source collection

	if err != nil {
		fmt.Println("error creating subscription", err)
	}

	fmt.Println("sub ID is", sub.ID())

	//To enable concurrency settings
	sub.ReceiveSettings.Synchronous = false
	// NumGoroutines determines the number of goroutines sub.Receive will spawn to pull messages
	sub.ReceiveSettings.NumGoroutines = g.source.Workers
	// MaxOutstandingMessages limits the number of concurrent handlers of messages.
	// In this case, up to 8 unacked messages can be handled concurrently.
	sub.ReceiveSettings.MaxOutstandingMessages = 8

	err = sub.Receive(g.ctx, func(_ context.Context, m *pubsub.Message) {
		fmt.Println("RECEIVING MESSAGE FROM GOOGLE PUB SUB >>>")
		fmt.Println(string(m.Data))
		g.sfc.mu.Lock()
		g.sfc.count++
		fmt.Println("total number of messages received\n", g.sfc.count)
		g.sfc.mu.Unlock()
		fmt.Printf("time :%v", time.Now().Format("Jan _2 15:04:05.000000"))
		m.Ack()
	})

	if err != nil {
		fmt.Println("sub.Receive returned error", err)
		return
	}

	return
}

func NewPubSub(source mongo.Source) (PubSub, error) {
	if source.Type == mongo.SQSPubSub {
		return newSqsPubSub(source), nil
	}

	if source.Type == mongo.GooglePubSub {
		return newGooglePubSub(source), nil
	}

	return nil, fmt.Errorf("source with pub sub type: %s is not supported", source.Type)
}
