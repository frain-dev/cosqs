package main

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

type SafeCounter struct {
	mu    sync.Mutex
	wc    map[string]int
	count int
}

func addProducerCommand() *cobra.Command {
	var accessKeyID, region, secretAccessKey, queueName string
	var rate int
	cmd := &cobra.Command{
		Use:   "produce",
		Short: "send events",
		RunE: func(cmd *cobra.Command, args []string) error {
			sess, err := session.NewSession(&aws.Config{
				Region:      aws.String(region),
				Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
			})

			if err != nil {
				fmt.Println("error with initializing session", err)
				return err
			}

			//create a service client
			svc := sqs.New(sess)
			url, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			})

			if err != nil {
				fmt.Println("failed to fetch queue url", err)
			}

			//get the queue url
			queueURL := url.QueueUrl
			fmt.Println("queue url is", *queueURL)

			sc := &SafeCounter{}

			var wg sync.WaitGroup
			wg.Add(rate)
			for i := 1; i <= rate; i++ {
				go sendMessage(svc, queueURL, sc, &wg)
			}

			wg.Wait()

			return nil
		},
	}

	cmd.Flags().StringVar(&accessKeyID, "access-key-id", "", "AWS Access Key ID")
	cmd.Flags().StringVar(&secretAccessKey, "secret-access-key", "", "Secret Access Key")
	cmd.Flags().StringVar(&queueName, "queue-name", "", "Queue Name")
	cmd.Flags().StringVar(&region, "region", "", "Region")
	cmd.Flags().IntVar(&rate, "rate", 1, "Rate")

	// mark flags as required
	cmd.MarkFlagRequired("access-key-id")
	cmd.MarkFlagRequired("secret-access-key")
	cmd.MarkFlagRequired("queue-name")

	return cmd
}

func sendMessage(svc *sqs.SQS, queueUrl *string, sc *SafeCounter, wg *sync.WaitGroup) {
	messageId := uuid.NewString()

	fmt.Println("message Id is", messageId)

	_, err := svc.SendMessage(&sqs.SendMessageInput{
		MessageGroupId: aws.String(uuid.NewString()),
		QueueUrl:       queueUrl,
		MessageBody:    aws.String(messageId),
	})

	if err != nil {
		fmt.Println("error with sending messages", err)
	}

	sc.mu.Lock()
	sc.count++
	fmt.Println("total number of messages sent\n", sc.count)
	sc.mu.Unlock()

	fmt.Println("Message sent successfully")
	wg.Done()
}

//1 worker 38s
//1 worker 58s
//5 workers 50s
//5 workers
