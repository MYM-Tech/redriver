// Package redriver allows in-code retry handling retry of SQS message, and partial failures in multi-message processing.
package redriver

import (
	"errors"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Redriver is the main struct used to store policy and redrive messages.
type Redriver struct {
	ConsumedQueueURL string
	Retries          uint64
}

type processResult struct {
	message events.SQSMessage
	err     error
}

// MessageProcessor is the required function signature for processors.
type MessageProcessor = func(event events.SQSMessage) error

func (redriver Redriver) deleteProcessedMessages(processedMessages *[]processResult, sqsConnector *sqs.SQS) error {
	for _, processedMessage := range *processedMessages {
		_, err := sqsConnector.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &redriver.ConsumedQueueURL,
			ReceiptHandle: &processedMessage.message.ReceiptHandle,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (redriver Redriver) processMessageAsync(message events.SQSMessage, processor MessageProcessor, processResultChannel chan<- processResult) {
	go func() {
		var processorError error
		for i := uint64(1); i <= redriver.Retries; i++ {
			processorError = processor(message)

			if processorError == nil {
				processResultChannel <- processResult{message, nil}
				return
			}
		}

		processResultChannel <- processResult{message, processorError}
	}()
}

// HandleMessages handles asynchronously all SQS messages, and deletes it when they are processed.
func (redriver Redriver) HandleMessages(messages []events.SQSMessage, processor MessageProcessor) error {
	if redriver.Retries < 1 {
		return errors.New("retries must be 1 or above")
	}

	awsSession, err := session.NewSession()
	if err != nil {
		return fmt.Errorf("can't create an AWS session, reason: %s", err.Error())
	}

	sqsConnector := sqs.New(awsSession)

	messagesCount := len(messages)
	var processedMessages []processResult
	var failures []processResult

	processResultChannel := make(chan processResult)
	defer close(processResultChannel)

	for _, message := range messages {
		redriver.processMessageAsync(message, processor, processResultChannel)
	}

	for i := 0; i < messagesCount; i++ {
		processResult := <-processResultChannel
		if processResult.err != nil {
			failures = append(failures, processResult)
			continue
		}
		processedMessages = append(processedMessages, processResult)
	}

	// All messages processed.
	if len(processedMessages) == messagesCount {
		return nil
	}

	// All messages failed.
	if len(failures) == messagesCount {
		return fmt.Errorf("all messages processing failed, %+v", failures)
	}

	if err := redriver.deleteProcessedMessages(&processedMessages, sqsConnector); err != nil {
		return err
	}

	return fmt.Errorf("%d messages failed, %+v", len(failures), failures)
}
