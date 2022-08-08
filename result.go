package redriver

import (
	"github.com/aws/aws-lambda-go/events"
)

type processResults struct {
	messagesCount  int
	failedResults  []*processResult
	successResults []*processResult
}

type processResult struct {
	message events.SQSMessage
	err     error
}

func newProcessResult(message events.SQSMessage, err error) *processResult {
	return &processResult{
		message: message,
		err:     err,
	}
}

func newProcessResults(messages []events.SQSMessage) *processResults {
	return &processResults{messagesCount: len(messages), failedResults: []*processResult{}, successResults: []*processResult{}}
}

func (r *processResults) addResult(newProcessResult *processResult) {
	if newProcessResult.err != nil {
		r.failedResults = append(r.failedResults, newProcessResult)
		return
	}

	r.successResults = append(r.successResults, newProcessResult)
}

func (r *processResults) hasOnlySuccessfulMessages() bool {
	return len(r.successResults) == r.messagesCount
}
