# Redriver

![CI](https://github.com/MYM-Tech/redriver/workflows/CI/badge.svg)
[![GoDoc](https://godoc.org/github.com/MYM-Tech/redriver?status.svg)](https://godoc.org/github.com/MYM-Tech/redriver)
[![Go Report Card](https://goreportcard.com/badge/github.com/MYM-Tech/redriver)](https://goreportcard.com/report/github.com/MYM-Tech/redriver)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Original problem

When you process messages from an SQS Queue, and your processing is not idempotent (processing multiple times the same message would have a negative, or unwanted behavior, like re-sending the same email).

You could use the redrive policy to a Dead Letter Queue alone, but what if you only have 1 out of 10 messages that fails in your messages batch ? If you return no errors, the SQS will delete the message. If you return one, your entire batch will go to the DLQ and therefor be re-processed when you will replay the queue.

## How does it work ?

You'll have to define the number of retries and the consumed queue URL.

Then Redriver will use the processing function you'll provide on each event, inside goroutines, and retry them the amount of times you specified (use 1 for a single try).

If everything works well, Redriver will return `nil` when all messages have been processed and you should make your handler return a non-error type so all messages will be deleted.

If some or every messages failed even after retrying them, Redriver will delete the correctly processed messages from the queue and return an error. You should return any error in your handler in this case so every unprocessed messages will be sent to the DLQ specified in your AWS SQS Redrive Policy.

## SQS Redrive Policy

Since this module allows you to do in-code retries, you should set the lambda `maxReceiveCount` parameter to 1 if you use retries in this module.

If you don't do so, the amount of retries done will be `maxReceiveCount * redriverRetries`. It could also be a strategy with "quick" retries done by this module in-code, and delayed replay using `maxReceiveCount`.

## How to use

The usage is pretty simple, wrap your message processor (it should implement the `MessageProcessor` interface) with the `Redriver.HandleMessages` function like this:

```go
package main

func myEventProcessor(event events.SQSMessage) error {
	// business code
	
	return nil
}

func HandleEvent(_ context.Context, sqsEvent events.SQSEvent) error {
	messageRedriver := redriver.Redriver{Retries: uint64(3), ConsumedQueueURL: "https://..."}
	
	return messageRedriver.HandleMessages(sqsEvent.Records, myEventProcessor)
}

func main() {
	lambda.Start(HandleEvent)
}
```

You may also wrap the processor in a closure to pass dependencies, or you to use a middleware:

```go
package main

func HandleEvent(_ context.Context, sqsEvent events.SQSEvent) error {
	messageRedriver := redriver.Redriver{Retries: uint64(3), ConsumedQueueURL: "https://..."}
	
	return messageRedriver.HandleMessages(sqsEvent.Records, func(event events.SQSMessage) error {
		fmt.Println("Start processing of a message")
		
		return myEventProcessor(event, myDependency1, myDependency2)
	})
}
```

Returning the error of the Redriver is a good practice (because of the explanation above about lambda error handling), and if you don't do so, you will anyway need to return an error from the main handler if the Redriver returned one.

The Redriver will fail early if it can't create an AWS session, or if the retry parameter is < 1.

## Note

Using `uint64` may seem a bit overkill but it allows for a *very* large amount of retries in case you need it.
