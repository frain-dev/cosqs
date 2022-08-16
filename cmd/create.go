package main

import (
	"context"
	"fmt"

	"github.com/frain-dev/cosqs/mongo"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

func addCreateCommand(a *application) *cobra.Command {
	var accessKeyID, secretAccessKey, defaultRegion, queueName string
	var workers int

	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a new source",
		RunE: func(cmd *cobra.Command, args []string) error {
			source := &mongo.Source{
				UID:             uuid.New().String(),
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
				DefaultRegion:   defaultRegion,
				QueueName:       queueName,
				Workers:         workers,
			}

			err := a.sourceRepo.CreateSource(context.Background(), source)
			if err != nil {
				return err
			}

			fmt.Printf("Source with ID: %s has been created", source.UID)
			return nil
		},
	}

	cmd.Flags().StringVar(&accessKeyID, "access-key-id", "", "AWS Access Key ID")
	cmd.Flags().StringVar(&secretAccessKey, "secret-access-key", "", "Secret Access Key")
	cmd.Flags().StringVar(&defaultRegion, "default-region", "", "Default Region")
	cmd.Flags().StringVar(&queueName, "queue-name", "", "Queue Name")
	cmd.Flags().IntVar(&workers, "workers", 1, "Workers per source")

	// mark flags as required
	cmd.MarkFlagRequired("access-key-id")
	cmd.MarkFlagRequired("secret-access-key")
	cmd.MarkFlagRequired("default-region")
	cmd.MarkFlagRequired("queue-name")

	return cmd
}
