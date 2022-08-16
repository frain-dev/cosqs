package main

import (
	"log"
	"os"
)

//fetch all the sources in the DB
// loop through each of them
// create a new goroutine that longs polls the sqs queue
//

//create new mongo connection
//add cmd command for creating source
//add queue command for polling sqs queue
//add server command for running metrics server

func main() {
	if err := Execute(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
