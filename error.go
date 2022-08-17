package redriver

import (
	"fmt"
)

type processResultsError struct {
	Results processResults
}

func (error processResultsError) Error() string {
	errMessage := fmt.Sprintln("messages processing failed :")

	for _, result := range error.Results.failedResults {
		errMessage += fmt.Sprintf("message : %s, error : %s\n", result.message.MessageId, result.err)
	}

	return errMessage
}
