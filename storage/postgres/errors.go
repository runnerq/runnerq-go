package postgres

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/runnerq/runnerq-go/storage"
)

// databaseError preserves the driver cause. Only known transient failures may
// be retried; malformed SQL, invalid JSON and constraints must surface instead.
func databaseError(err error, message string) error {
	kind := storage.ErrInternal
	var pgErr *pgconn.PgError
	var connErr *pgconn.ConnectError
	var netErr net.Error
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		kind = storage.ErrTimeout
	case errors.As(err, &pgErr):
		switch pgErr.Code {
		case "40001", "40P01", "55P03":
			kind = storage.ErrConflict
		case "08000", "08001", "08003", "08004", "08006", "08007", "08P01", "57P01", "57P02", "57P03", "53300":
			kind = storage.ErrUnavailable
		}
	case errors.As(err, &connErr), errors.As(err, &netErr), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, pgx.ErrTxClosed):
		kind = storage.ErrUnavailable
	}
	return &storage.StorageError{Kind: kind, Message: message, Cause: err}
}
