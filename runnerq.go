// Package runnerq is a durable activity queue and worker engine, with
// pluggable storage (PostgreSQL built in).
//
// Activities run by priority and schedule, retry with exponential backoff and
// dead-letter when out of attempts. Handlers can checkpoint steps (Run,
// RunStep), sleep and wait for signals durably, and spawn and await child
// activities. The conductor package connects workers to a console.
//
//	backend, _ := postgres.New(ctx, "postgres://localhost/mydb", "my_app")
//	engine, _ := runnerq.Builder().
//	    Backend(backend).
//	    QueueName("my_app").
//	    MaxWorkers(8).
//	    Build()
//
//	engine.RegisterActivity(&SendEmail{}) // serves activity type "SendEmail"
//	engine.Start(ctx)
package runnerq
