package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/frain-dev/cosqs/mongo"
	"github.com/spf13/cobra"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dchest/uniuri"
)

var (
	sfc          = &SafeCounter{wc: make(map[string]int)}
	sourceStream = make(map[string]mongo.Source)
)

/**
1. How do we scale the ingesters?
We can allow users configure the number of workers/goroutine running per source which is similiar to
increasing the number of threads per client.
 - How do we prevent multiple workers/goroutine from consuming the same message?
 -
2. Increase the number of clients(ingest process) running
3. How do we measure execution time? If I send 10K messages, how long does it take to process?
**/
func addIngesterCommand(a *application) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest",
		Short: "convoy ingester",
		RunE: func(cmd *cobra.Command, args []string) error {

			ticker := time.NewTicker(1 * time.Minute)

			for range ticker.C {
				err := discoverSources(a.sourceRepo)

				if err != nil {
					fmt.Println("error with fetching sources,", err)
				}
			}

			return nil
		},
	}

	return cmd
}

func discoverSources(sourceRepo mongo.SourceRepository) error {
	sources, err := sourceRepo.LoadSources(context.Background())
	if err != nil {
		return err
	}

	for _, source := range sources {
		_, exists := sourceStream[source.UID]

		// We've seen this source already
		if exists {
			continue
		}

		dispatchSource(source)
		sourceStream[source.UID] = source

	}

	return nil
}

func dispatchSource(source mongo.Source) {
	for i := 1; i <= source.Workers; i++ {
		go setupQueueListener(source)
	}
}

//Sets up a new queue listener
func setupQueueListener(source mongo.Source) {
	id := uniuri.NewLen(6)
	fmt.Printf("Running worker with ID: %s\n", id)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(source.DefaultRegion),
		Credentials: credentials.NewStaticCredentials(source.AccessKeyID, source.SecretAccessKey, ""),
	})

	if err != nil {
		fmt.Println("error with initializing session", err)
	}

	//create a service client
	svc := sqs.New(sess)
	url, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &source.QueueName,
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
				sfc.mu.Lock()
				sfc.count++
				fmt.Println("total number of messages received\n", sfc.count)
				sfc.mu.Unlock()
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
