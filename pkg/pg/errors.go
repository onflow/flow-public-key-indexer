package pg

import (
	"errors"

	"github.com/go-pg/pg/v10"
)

var (
	// ErrNoRows is returned by QueryOne and ExecOne when query returned zero rows
	// but at least one row is expected.
	ErrNoRows = errors.New("pg: at least one row expected, none returned")
	// ErrMultiRows is returned by QueryOne and ExecOne when query returned
	// multiple rows but exactly one row is expected.
	ErrMultiRows = errors.New("pg: many rows returned, one expected")
	// ErrInvalidEnumValue typical error when the enum value is invalid
	ErrInvalidEnumValue = errors.New("pg: invalid value for enum")
	// ErrIntegrityViolation is returned when an integrity constraint is violated
	ErrIntegrityViolation = errors.New("pg: integrity violation")
)

func convertError(err error) error {
	switch err {
	case pg.ErrMultiRows:
		return ErrMultiRows
	case pg.ErrNoRows:
		return ErrNoRows
	case ErrNoRows, ErrMultiRows, ErrInvalidEnumValue, nil:
		return err
	default:
		pgErr, ok := err.(pg.Error)
		if ok && pgErr.IntegrityViolation() {
			return ErrIntegrityViolation
		}
		return err
	}
}
