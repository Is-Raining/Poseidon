package dao

import (
	"context"
	"database/sql"
	"fmt"

	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"git.woa.com/yt-industry-ai/poseidon/pkg/utils"
	"github.com/jmoiron/sqlx"
)

// GetCountContext ...
func GetCountContext(
	ctx context.Context,
	query string,
	args []interface{},
	db *sqlx.DB,
) (count uint32, err error) {
	tc := utils.NewTicker()
	logger := common.GetLogger(ctx)

	clean := func() {
		logger.Debugf("SQL EXECL. err = %v, cost = %v, sql: %s", err, tc.Tock(), query)
	}
	defer clean()

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		logger.WithError(err).Errorf("prepareContext fail")
		return 0, common.WrapDBError(ctx, err, code.Code_InternalError__QueryTable)
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(ctx, args...).Scan(&count)
	if err != nil {
		logger.WithError(err).Errorf("QueryRowContext fail")
		return 0, common.WrapDBError(ctx, err, code.Code_InternalError__QueryTable)
	}

	return count, nil
}

// Select multiple rows ...
func SelectContext(ctx context.Context,
	query string,
	tx *sqlx.Tx,
	db *sqlx.DB,
	dest interface{},
	args []interface{}) (err error) {
	tc := utils.NewTicker()
	logger := common.GetLogger(ctx)

	clean := func() {
		logger.Debugf("SQL EXECL. err = %v, cost = %v, sql: %s", err, tc.Tock(), query)
	}
	defer clean()

	// prepare
	stmt, err := prepareContext(ctx, query, tx, db)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// query
	err = stmt.SelectContext(ctx, dest, args...)
	if err != nil {
		logger.WithError(err).Errorf("select fail")
		return common.Errorf(ctx, code.Code_InternalError__QueryTable)
	}
	return nil
}

// Get one row ...
func GetContext(ctx context.Context,
	query string,
	tx *sqlx.Tx,
	db *sqlx.DB,
	dest interface{},
	args []interface{}) (err error) {
	tc := utils.NewTicker()
	logger := common.GetLogger(ctx)

	clean := func() {
		logger.Debugf("SQL EXECL. err = %v, cost = %v, sql: %s", err, tc.Tock(), query)
	}
	defer clean()

	// prepare
	stmt, err := prepareContext(ctx, query, tx, db)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// query
	err = stmt.GetContext(ctx, dest, args...)
	if err == sql.ErrNoRows {
		return common.Errorf(ctx, code.Code_NOT_FOUND)
	}

	if err != nil {
		logger.WithError(err).Errorf("select fail")
		return common.Errorf(ctx, code.Code_InternalError__QueryTable)
	}
	return nil
}

// ExecContext ...
func ExecContext(
	ctx context.Context,
	query string,
	args []interface{},
	tx *sqlx.Tx,
	db *sqlx.DB,
) (result sql.Result, err error) {
	tc := utils.NewTicker()
	logger := common.GetLogger(ctx)

	clean := func() {
		logger.Debugf("SQL EXECL. err = %v, cost = %v, sql: %s", err, tc.Tock(), query)
	}
	defer clean()

	result, err = execContext(ctx, query, args, tx, db)
	if err != nil {
		logger.WithError(err).Errorf("execute fail")
		return nil, common.WrapDBError(ctx, err, code.Code_InternalError__ExecTable)
	}
	return result, nil
}

func execContext(
	ctx context.Context,
	query string,
	args []interface{},
	tx *sqlx.Tx,
	db *sqlx.DB,
) (result sql.Result, err error) {
	if tx != nil {
		if args != nil {
			result, err = tx.ExecContext(ctx, query, args...)
		} else {
			result, err = tx.ExecContext(ctx, query)
		}
	} else if db != nil {
		if args != nil {
			result, err = db.ExecContext(ctx, query, args...)
		} else {
			result, err = db.ExecContext(ctx, query)
		}
	} else {
		return nil, fmt.Errorf("tx and db both nil")
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func prepareContext(ctx context.Context,
	query string,
	tx *sqlx.Tx,
	db *sqlx.DB,
) (*sqlx.Stmt, error) {
	logger := common.GetLogger(ctx)

	var stmt *sqlx.Stmt = nil
	var err error = nil
	if tx != nil {
		stmt, err = tx.PreparexContext(ctx, query)
	} else if db != nil {
		stmt, err = db.PreparexContext(ctx, query)
	} else {
		logger.Errorf("db and tx are nil")
		return nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
	}
	if err != nil {
		logger.WithError(err).Errorf("prepare statement fail")
		return nil, common.Errorf(ctx, code.Code_InternalError__QueryTable)
	}
	return stmt, nil
}
