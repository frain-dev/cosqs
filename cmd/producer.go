package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/frain-dev/cosqs/mongo"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

type SafeCounter struct {
	mu    sync.Mutex
	wc    map[string]int
	count int
}

type Producer interface {
	BroadCast() error
}

type SQSProducer struct {
	rate     int
	svc      *sqs.SQS
	queueURL *string
	sc       *SafeCounter
	wg       *sync.WaitGroup
}

func newSqsProducer(accessKeyID, secretAccessKey, region, queueName string, rate int) (Producer, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
	})

	if err != nil {
		fmt.Println("error with initializing session", err)
		return nil, err
	}

	//create a service client
	svc := sqs.New(sess)
	url, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		fmt.Println("failed to fetch queue url", err)
		return nil, err
	}

	s := &SQSProducer{
		rate:     rate,
		svc:      svc,
		queueURL: url.QueueUrl,
		sc:       &SafeCounter{wc: make(map[string]int)},
		wg:       &sync.WaitGroup{},
	}

	return s, nil
}

func (s *SQSProducer) BroadCast() error {
	s.wg.Add(s.rate)

	for i := 1; i <= s.rate; i++ {
		go s.dispatch()
	}

	s.wg.Wait()

	return nil
}

func (s *SQSProducer) dispatch() error {
	messageId := uuid.NewString()

	fmt.Println("message Id is", messageId)

	defer s.wg.Done()

	_, err := s.svc.SendMessage(&sqs.SendMessageInput{
		MessageGroupId: aws.String(uuid.NewString()),
		QueueUrl:       s.queueURL,
		MessageBody:    aws.String(messageId),
	})

	if err != nil {
		fmt.Println("error with sending messages", err)
	}

	s.sc.mu.Lock()
	s.sc.count++
	fmt.Println("total number of messages sent\n", s.sc.count)
	s.sc.mu.Unlock()

	fmt.Println("Message sent successfully")
	return nil
}

type GoogleProducer struct {
	rate   int
	client *pubsub.Client
	sc     *SafeCounter
	wg     *sync.WaitGroup
	topic  *pubsub.Topic
}

func newGoogleProducer(apiKey, projectID, topicName string, rate int) (Producer, error) {
	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		fmt.Println("error with setting up client", err)
		return nil, err
	}

	//create a topic
	topic := client.Topic(topicName)

	g := &GoogleProducer{
		rate:   rate,
		client: client,
		sc:     &SafeCounter{wc: make(map[string]int)},
		wg:     &sync.WaitGroup{},
		topic:  topic,
	}

	return g, nil
}

func (g *GoogleProducer) BroadCast() error {
	ctx := context.Background()

	var totalErrors uint64

	for i := 1; i <= g.rate; i++ {
		messageId := uuid.NewString()

		result := g.topic.Publish(ctx, &pubsub.Message{
			Data: []byte(messageId),
		})

		g.wg.Add(1)
		go func(i int, res *pubsub.PublishResult) {
			defer g.wg.Done()

			id, err := res.Get(ctx)
			if err != nil {
				fmt.Println("failed to publish", err)
				atomic.AddUint64(&totalErrors, 1)
				return
			}
			fmt.Printf("Published message: %d; msg ID: %v\n", i, id)
		}(i, result)
	}

	g.wg.Wait()

	if totalErrors > 0 {
		return fmt.Errorf("%d of %d messages did not publish successfully", totalErrors, g.rate)
	}

	return nil

}

func addProducerCommand() *cobra.Command {
	var accessKeyID, region, secretAccessKey, queueName, pubType string
	var projectID, apiKey, topicName string
	var rate int
	cmd := &cobra.Command{
		Use:   "produce",
		Short: "send events",
		RunE: func(cmd *cobra.Command, args []string) error {
			var p Producer
			var err error

			if mongo.PubSubType(pubType) == mongo.SQSPubSub {
				p, err = newSqsProducer(accessKeyID, secretAccessKey, region, queueName, rate)
				if err != nil {
					return err
				}
			} else if mongo.PubSubType(pubType) == mongo.GooglePubSub {
				p, err = newGoogleProducer(apiKey, projectID, topicName, rate)
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("pub-sub with type: %s is not supported", pubType)
			}

			return p.BroadCast()
		},
	}

	cmd.Flags().StringVar(&accessKeyID, "access-key-id", "", "AWS Access Key ID")
	cmd.Flags().StringVar(&secretAccessKey, "secret-access-key", "", "Secret Access Key")
	cmd.Flags().StringVar(&queueName, "queue-name", "", "Queue Name")
	cmd.Flags().StringVar(&region, "region", "", "Region")
	cmd.Flags().IntVar(&rate, "rate", 1, "Rate")
	cmd.Flags().StringVar(&pubType, "type", "sqs", "Pub-Sub Type")
	cmd.Flags().StringVar(&projectID, "project-id", "", "Google Project ID")
	cmd.Flags().StringVar(&apiKey, "api-key", "", "Google API Key")
	cmd.Flags().StringVar(&topicName, "topic-name", "", "Topic Name")

	return cmd
}
