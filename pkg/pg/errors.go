package pg

import (
	"database/sql"
	"errors"
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
)

func convertError(err error) error {
	switch err {
	case sql.ErrNoRows:
		return ErrNoRows
	case ErrNoRows, ErrMultiRows, ErrInvalidEnumValue, nil:
		return err
	default:
		return err
	}
}
