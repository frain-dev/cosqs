package main

import (

	"github.com/frain-dev/cosqs/mongo"
	"github.com/spf13/cobra"
)

type application struct {
	sourceRepo mongo.SourceRepository
}

var app = &application{}

var (
	// used for flags
	dsn     string
	client  *mongo.Client
	rootCmd = &cobra.Command{
		Use:   "cosqs",
		Short: "Cosqs is convoy's prototype for pub/sub",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error

			client, err = mongo.New(dsn)
			if err != nil {
				return err
			}

			app.sourceRepo = client.SourceRepo
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dsn, "dsn", "mongodb://root:rootpassword@localhost:27017/cosqs?authSource=admin", "Mongo DB DSN")

	rootCmd.AddCommand(addCreateCommand(app))
	rootCmd.AddCommand(addIngesterCommand(app))
	rootCmd.AddCommand(addProducerCommand())
}
