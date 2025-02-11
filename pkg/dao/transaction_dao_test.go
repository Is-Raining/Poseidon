package dao

import (
	"testing"

	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

type TransactionDaoTestSuite struct {
	suite.Suite
	db *sqlx.DB
}

func (s *TransactionDaoTestSuite) SetupSuite() {
	s.db = GetTestDBOrDie("../../..")

	// create t_test_user
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS t_test_user (
		name varchar(64) not null default '',
    	PRIMARY KEY (name)
		) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;`)
	s.Nil(err)

	// truncate
	_, err = s.db.Exec("TRUNCATE t_test_user")
	s.Nil(err)

	// insert
	_, err = s.db.Exec(`INSERT INTO t_test_user(name) VALUES ("x"),("y");`)
	s.Nil(err)
}

func (s *TransactionDaoTestSuite) TearDownSuite() {
	// drop t_test_user
	_, err := s.db.Exec("DROP TABLE t_test_user")
	s.Nil(err)
}

func (s *TransactionDaoTestSuite) TestTransaction() {
	ctx, cancel := common.NewTestContext("test", "test")
	defer cancel()

	err := TxPipelined(ctx, s.db, func(tx *sqlx.Tx) error {
		_, err := ExecContext(ctx, `INSERT INTO t_test_user(name) VALUES ("x")`, nil, tx, nil)
		if err != nil {
			return err
		}
		_, err = ExecContext(ctx, `UPDATE t_test_user SET name = "z" WHERE name = "y"`, nil, tx, nil)
		if err != nil {
			return err
		}
		return nil
	})
	s.NotNil(err)

	rows := []*TestUser{}
	err = SelectContext(ctx, `SELECT * FROM t_test_user WHERE name = "z"`, nil, s.db, &rows, nil)
	s.Nil(err)
	s.Len(rows, 0)
}

func TestTransactionDaoTestSuite(t *testing.T) {
	suite.Run(t, new(TransactionDaoTestSuite))
}
