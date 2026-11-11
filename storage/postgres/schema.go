package postgres

import (
	"strings"

	"github.com/runnerq/runnerq-go/internal/spec"
)

// schemaSql is every runnerq-spec migration, run as one multi-statement exec
// and so in one transaction. Each is safe to re-run.
var schemaSql = func() string {
	var b strings.Builder
	for _, m := range spec.PostgresMigrations {
		b.WriteString(m.SQL)
		b.WriteString("\n")
	}
	return b.String()
}()
