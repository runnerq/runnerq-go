package storage

import (
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/internal/spec"
)

const businessKeyPrefix = spec.BusinessKeyPrefix

// StepKeyPrefix starts the keys the engine derives for activities spawned by
// a step; they are not application keys.
const StepKeyPrefix = spec.StepKeyPrefix

// StepIdempotencyKey is the key of the activity a step spawns under parent in
// root's tree. A retried parent re-issuing the spawn derives the same key and
// gets the existing child.
func StepIdempotencyKey(root, parent uuid.UUID, step string) string {
	return StepKeyPrefix + root.String() + ":" + parent.String() + ":" + step
}

// BusinessIdempotencyKey encodes (key, activityType) unambiguously. Base64
// has no '-', so a v2 key never equals a legacy "<key>-<type>" key or an
// rq:step key (which contains UUIDs).
func BusinessIdempotencyKey(key, activityType string) string {
	data := strconv.Itoa(len(key)) + ":" + key + activityType
	return businessKeyPrefix + base64.RawStdEncoding.EncodeToString([]byte(data))
}

// LegacyBusinessIdempotencyKey maps a v2 key to its legacy "<key>-<type>"
// form for reading pre-v2 claims. Accept a legacy row only after verifying its
// activity type.
func LegacyBusinessIdempotencyKey(encoded string) (legacy, activityType string, ok bool) {
	key, typ, ok := decodeBusinessKey(encoded)
	if !ok {
		return "", "", false
	}
	return key + "-" + typ, typ, true
}

// ApplicationIdempotencyKey recovers the application's key from a stored key
// and activity type, as QueryStorage reports it. Handles v2 and legacy
// "<key>-<type>" keys; raw storage-API keys are returned as is, and step-derived
// keys yield "".
func ApplicationIdempotencyKey(stored, activityType string) string {
	if stored == "" || strings.HasPrefix(stored, StepKeyPrefix) {
		return ""
	}
	if key, typ, ok := decodeBusinessKey(stored); ok {
		if typ == activityType {
			return key
		}
		return stored
	}
	if key, ok := strings.CutSuffix(stored, "-"+activityType); ok && key != "" && activityType != "" {
		return key
	}
	return stored
}

func decodeBusinessKey(encoded string) (key, activityType string, ok bool) {
	if !strings.HasPrefix(encoded, businessKeyPrefix) {
		return "", "", false
	}
	data, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(encoded, businessKeyPrefix))
	if err != nil {
		return "", "", false
	}
	colon := strings.IndexByte(string(data), ':')
	if colon < 0 {
		return "", "", false
	}
	n, err := strconv.Atoi(string(data[:colon]))
	if err != nil || n < 0 || n > len(data)-colon-1 {
		return "", "", false
	}
	key, typ := string(data[colon+1:colon+1+n]), string(data[colon+1+n:])
	if BusinessIdempotencyKey(key, typ) != encoded {
		return "", "", false
	}
	return key, typ, true
}
