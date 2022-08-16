package main

import (
	"context"
	"fmt"
	"sync"
	"time"
	_ "time"

	"github.com/frain-dev/cosqs/mongo"
	"github.com/spf13/cobra"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dchest/uniuri"
)

type Cosqs struct {
	message  *sqs.Message
	queueUrl string
	id       string
	sqs      *sqs.SQS
}

var (
	chanMessages    = make(chan *Cosqs)
	chanDelMessages = make(chan *Cosqs)
	sfc             = &SafeCounter{wc: map[string]int{}}
)

// map {source_id: 9}

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
			sources, err := a.sourceRepo.LoadSources(context.Background())
			if err != nil {
				return err
			}

			for _, source := range sources {
				var wg sync.WaitGroup

				//multiple clients
				//multiple threads per client

				if source.Workers > 1 {
					wg.Add(source.Workers)
					for i := 1; i <= source.Workers; i++ {
						go setupQueueListener(source, &wg)
					}

					wg.Wait()
				} else {
					fmt.Println("here2")
					go setupQueueListener(source, nil)
				}
			}

			// ticker := time.NewTicker(1 * time.Second)

			for message := range chanMessages {
				handleMessage(message)
				deleteMessage(message)
				// emitMessage(message)
			}

			return nil
		},
	}

	return cmd
}

//Sets up a new queue listener
func setupQueueListener(source mongo.Source, wg *sync.WaitGroup) {
	// fmt.Printf("Setting up queue listener for source with ID :%s\n", source.UID)
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

	if wg != nil {
		wg.Done()
	}

	// for i := 1; i <= source.Workers; i++ {
	// 	go handle(id, chanMessages, chanDelMessages)
	// }

	// go handle(id, chanMessages, chanDelMessages)

	//infinite loop to poll messages from the queue
	for {
		// fmt.Println("here for loop")
		output, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            queueURL,
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64(1),
		})

		// fmt.Printf("worker UID: %s\n, messages length: %v\n, received time: %v\n", id, len(output.Messages), time.Now().Format("Jan _2 15:04:05.000000"))

		if err != nil {
			fmt.Printf("failed to fetch sqs message %v", err)
		}

		var wgg sync.WaitGroup
		for _, message := range output.Messages {
			wgg.Add(1)
			go func(m *sqs.Message) {
				defer wgg.Done()
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
				// chanMessages <- &Cosqs{message: m, queueUrl: *queueURL, sqs: svc, id: id}
			}(message)

			// go func(m *sqs.Message) {
			// }(message)

			wgg.Wait()
			// chanMessages <- &Cosqs{message: message, queueUrl: *queueURL, sqs: svc, id: id}
		}
	}
}

func handle(id string, cm <-chan *Cosqs, cdm chan<- *Cosqs) {
	for m := range cm {
		// fmt.Println("here with worker", m.id)
		// fmt.Println("RECEIVING MESSAGE >>>")
		// fmt.Println(*m.message.Body)
		sfc.mu.Lock()
		// sfc.count++
		sfc.wc[m.id]++
		exists, ok := sfc.wc[m.id]

		if ok {
			fmt.Printf("Worker UID: %v total number of messages received:%v\n", m.id, exists)

		}

		sfc.mu.Unlock()
		cdm <- m
	}
}

func handleMessage(data *Cosqs) {
	// fmt.Println(*data.message.Body)
	fmt.Printf("processed by worker with ID:%s\v :time: %v\n", data.id, time.Now().Format("Jan _2 15:04:05.000000"))

}

func emitMessage(data *Cosqs) {
	sfc.mu.Lock()
	sfc.wc[data.id]++
	sfc.mu.Unlock()
	fmt.Printf("map: %v\n, time: %v\n", sfc.wc, time.Now())

}

// delete message from queue
func deleteMessage(data *Cosqs) {
	data.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &data.queueUrl,
		ReceiptHandle: data.message.ReceiptHandle,
	})
}
