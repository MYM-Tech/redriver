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
	Debug            bool
}

// MessageProcessor is the required function signature for processors.
type MessageProcessor = func(event events.SQSMessage) error

func (redriver Redriver) deleteProcessedMessages(processedMessages processResults, sqsConnector *sqs.SQS) error {
	for _, processedMessage := range processedMessages.successResults {
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

func (redriver Redriver) processMessageAsync(message events.SQSMessage, processor MessageProcessor, processResultChannel chan<- *processResult) {
	go func() {
		var processorError error
		for i := uint64(1); i <= redriver.Retries; i++ {
			processorError = processor(message)

			if processorError == nil {
				break
			}
		}

		processResultChannel <- newProcessResult(message, processorError)
	}()
}

// HandleMessages handles asynchronously all SQS messages, and deletes it when they are processed.
func (redriver Redriver) HandleMessages(messages []events.SQSMessage, processor MessageProcessor) error {
	if redriver.Retries < 1 {
		return errors.New("retries must be 1 or above")
	}

	var sqsConnector *sqs.SQS
	if !redriver.Debug {
		awsSession, err := session.NewSession()
		if err != nil {
			return fmt.Errorf("can't create an AWS session, reason: %s", err.Error())
		}
		sqsConnector = sqs.New(awsSession)
	}

	processedMessagesResult := newProcessResults(messages)
	processResultChannel := make(chan *processResult)
	defer close(processResultChannel)

	for _, message := range messages {
		redriver.processMessageAsync(message, processor, processResultChannel)
	}

	for i := 0; i < processedMessagesResult.messagesCount; i++ {
		res := <-processResultChannel
		processedMessagesResult.addResult(res)
	}

	if processedMessagesResult.hasOnlySuccessfulMessages() {
		return nil
	}

	if err := redriver.deleteProcessedMessages(*processedMessagesResult, sqsConnector); !redriver.Debug && err != nil {
		return err
	}

	return processResultsError{Results: *processedMessagesResult}
}
