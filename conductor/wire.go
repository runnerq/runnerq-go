package conductor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/runnerq/runnerq-go/conductor/internal/wire"
)

// wireError is a protocol error: a handler returns one to choose the code.
type wireError wire.Error

func (e *wireError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func errorf(code wire.ErrorCode, format string, args ...any) *wireError {
	return &wireError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func fieldError(code wire.ErrorCode, field, format string, args ...any) *wireError {
	e := errorf(code, format, args...)
	e.Details = map[string]any{"field": field}
	return e
}

// unknownField names the field encoding/json rejected under
// DisallowUnknownFields, which reports it only in the message.
func unknownField(err error) (string, bool) {
	rest, ok := strings.CutPrefix(err.Error(), "json: unknown field ")
	if !ok {
		return "", false
	}
	field, err := strconv.Unquote(rest)
	return field, err == nil
}
