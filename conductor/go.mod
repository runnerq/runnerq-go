module github.com/alob-mtc/runnerq-go/conductor

go 1.27

require (
	github.com/alob-mtc/runnerq-go v0.5.2-0.20261003231333-64e2060f8037
	github.com/coder/websocket v1.8.15
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.8.0
	github.com/runnerq/runnerq-spec v0.4.1-0.20261002221630-92373c47b3f6
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

// Developed with the SDK in this repository; released alongside it.
replace github.com/alob-mtc/runnerq-go => ../
