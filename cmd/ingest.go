package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/frain-dev/cosqs/mongo"
	"github.com/frain-dev/cosqs/pubsub"
	"github.com/spf13/cobra"
)

var (
	wp = &WorkerPool{s: make(map[string]*SourceStream)}
)

type SourceStream struct {
	source mongo.Source
	client pubsub.PubSub // the pub sub client
}

type WorkerPool struct {
	s map[string]*SourceStream
}

func (w *WorkerPool) Insert(source mongo.Source, client pubsub.PubSub) {
	w.s[source.UID] = &SourceStream{source: source, client: client}
}

func (w *WorkerPool) Stop(sourceId string) {
	w.s[sourceId].client.Stop()
}

func (w *WorkerPool) Remove(sourceId string) {
	delete(w.s, sourceId)
}

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
			rTicker := time.NewTicker(2 * time.Second)

			for {
				select {
				case <-ticker.C:
					err := discoverSources(a.sourceRepo)
					if err != nil {
						fmt.Println("error fetching sources", err)
					}
				case <-rTicker.C:
					//prints the number of goroutines running
					fmt.Println("num of goroutines running:", runtime.NumGoroutine())
				}
			}
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
		w, exists := wp.s[source.UID]

		// We've seen this source already
		if exists {
			//the number of workers has changed
			if w.source.Workers != source.Workers {
				fmt.Println("workers has changed")
				wp.Stop(source.UID)
				wp.Remove(source.UID)
			}

			continue
		}

		ps, err := pubsub.NewPubSub(source)
		if err != nil {
			fmt.Println("error with pub sub", err)
			continue
		}

		wp.Insert(source, ps)
		ps.Dispatch()

	}

	return nil
}
