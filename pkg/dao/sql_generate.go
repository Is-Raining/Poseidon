package dao

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

const dbTag = "db"

// SqlCompare ...
type SqlCompare string

// SqlCompareEqual ...
const (
	SqlCompareEqual      SqlCompare = "="
	SqlCompareNotEqual   SqlCompare = "!="
	SqlCompareGreater    SqlCompare = ">"
	SqlCompareLessThan   SqlCompare = "<"
	SqlCompareGreatEqual SqlCompare = ">="
	SqlCompareLessEqual  SqlCompare = "<="
	SqlCompareLike       SqlCompare = "LIKE"
	SqlCompareIn         SqlCompare = "IN"
	SqlCompareLogicAnd   SqlCompare = "&"
)

// SqlOperator ...
type SqlOperator string

// SqlOperatorAnd ...
const (
	SqlOperatorAnd SqlOperator = "AND"
	SqlOperatorOr  SqlOperator = "OR"
	SqlOperatorNot SqlOperator = "NOT"
)

// packed condition
func PackedCondition(
	ctx context.Context,
	eqConds, inConds, likeConds []string,
	currentProject, publicProject bool,
	arg interface{},
) (cond string, args []interface{}, err error) {
	logger := common.GetLogger(ctx)

	conds := []string{}

	// equal
	if len(eqConds) > 0 {
		eqCond, eqArgs, err := sqlx.Named(ConditionWithEqualAnd(eqConds...), arg)
		if err != nil {
			logger.WithError(err).Errorf("build eq cond fail")
			return "", nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
		}
		conds = append(conds, eqCond)
		args = append(args, eqArgs...)
	}

	// in
	if len(inConds) > 0 {
		inCond, inArgs, err := ConditionWithInAnd(inConds, arg)
		if err != nil {
			logger.WithError(err).Errorf("build in condition fail")
			return "", nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
		}
		conds = append(conds, inCond)
		args = append(args, inArgs...)
	}

	// like
	if len(likeConds) > 0 {
		likeCond, likeArgs, err := sqlx.Named(ConditionWithLikeAnd(likeConds...), arg)
		if err != nil {
			logger.WithError(err).Errorf("build like condition fail")
			return "", nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
		}
		conds = append(conds, likeCond)
		args = append(args, likeArgs...)
	}

	// project
	projectIds := []interface{}{}
	if currentProject {
		projectIds = append(projectIds, common.GetProjectId(ctx))
	}
	if publicProject {
		projectIds = append(projectIds, common.PUBLIC_PROJ)
	}
	if len(projectIds) > 0 {
		projectCond, projectArgs, err := ConditionWithInAnd([]string{"project_id"}, map[string]interface{}{
			"project_id": projectIds,
		})
		if err != nil {
			logger.WithError(err).Errorf("build project condition fail")
			return "", nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
		}
		conds = append(conds, projectCond)
		args = append(args, projectArgs...)
	}

	if len(conds) > 0 {
		cond = strings.Join(conds, " AND ")
	} else {
		cond = "TRUE"
	}
	return cond, args, nil
}

// "ORDER BY foo LIMIT 10 OFFSET 100"
func PageAndOrder(
	offset, limit int32,
	orders map[string]Order,
) string {
	query := OrderCondition(orders)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset >= 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}
	return query
}

// "foo=:foo AND bar=:bar"
func ConditionWithEqualAnd(condFields ...string) string {
	return JoinNamedColumnsValuesWithOperator(SqlCompareEqual, SqlOperatorAnd, condFields...)
}

// "foo like :foo AND bar like :bar"
func ConditionWithLikeAnd(condFields ...string) string {
	return JoinNamedColumnsValuesWithOperator(SqlCompareLike, SqlOperatorAnd, condFields...)
}

// `foo in ("a","b",...)``
func ConditionWithInAnd(cols []string, arg interface{}) (query string, args []interface{}, err error) {
	return NamedInCondition(SqlOperatorAnd, cols, arg)
}

// `1 <= foo AND foo < 100`
func ConditionWithRangeAnd(cols []string, arg interface{}) (query string, args []interface{}, err error) {
	conds := []string{}
	for _, col := range cols {
		cond, tmpArgs, err := RangeCondition(col, arg)
		if err != nil {
			return "", nil, err
		}
		conds = append(conds, cond)
		args = append(args, tmpArgs...)
	}
	query = strings.Join(conds, fmt.Sprintf(" %s ", SqlOperatorAnd))
	return query, args, nil
}

// `1 <= foo AND foo < 100`
func RangeCondition(col string, arg interface{}) (query string, args []interface{}, err error) {
	_, tmpArgs, err := sqlx.Named(":"+col, arg)
	if err != nil {
		return "", nil, err
	}
	if len(tmpArgs) != 1 {
		return "", nil, fmt.Errorf("value not exist")
	}
	if v, ok := asSliceForIn(tmpArgs[0]); ok {
		if v.Len() != 2 {
			return "", nil, fmt.Errorf("value is not slice with len = 2")
		}
		args = appendReflectSlice(args, v, 2)
		return fmt.Sprintf("? <= %[1]s AND %[1]s < ?", col), args, nil
	} else {
		return "", nil, fmt.Errorf("value is not slice")
	}
}

func asSliceForIn(i interface{}) (v reflect.Value, ok bool) {
	if i == nil {
		return reflect.Value{}, false
	}

	v = reflect.ValueOf(i)
	t := reflectx.Deref(v.Type())

	// Only expand slices
	if t.Kind() != reflect.Slice {
		return reflect.Value{}, false
	}

	// []byte is a driver.Value type so it should not be expanded
	if t == reflect.TypeOf([]byte{}) {
		return reflect.Value{}, false

	}

	return v, true
}

func appendReflectSlice(args []interface{}, v reflect.Value, vlen int) []interface{} {
	switch val := v.Interface().(type) {
	case []interface{}:
		args = append(args, val...)
	case []int:
		for i := range val {
			args = append(args, val[i])
		}
	case []string:
		for i := range val {
			args = append(args, val[i])
		}
	default:
		for si := 0; si < vlen; si++ {
			args = append(args, v.Index(si).Interface())
		}
	}

	return args
}

// "ORDER BY create_time DESC, id DESC"
func OrderCondition(orders map[string]Order) string {
	if len(orders) == 0 {
		return ""
	}

	return fmt.Sprintf(" ORDER BY %s", func() string {
		var msg string
		for k, v := range orders {
			msg += fmt.Sprintf("%s %s,", k, func() string {
				return v.String()
			}())
		}

		msg = strings.TrimRight(msg, ",")
		return msg
	}())

}

// NamedInCondition ...
func NamedInCondition(oper SqlOperator, cols []string, arg interface{}) (query string, args []interface{}, err error) {
	if len(cols) == 0 {
		return "TRUE", args, nil
	}
	query = JoinNamedColumnsValuesWithOperator(SqlCompareIn, oper, cols...)
	query, args, err = sqlx.Named(query, arg)
	if err != nil {
		return "", nil, err
	}
	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return "", nil, err
	}

	return query, args, nil
}

// foo=:foo,bar=:bar, for update set
func JoinNamedColumnsValues(cols ...string) string {
	return strings.Join(namedTableColumnsValues(SqlCompareEqual, cols...), ",")
}

// "foo,bar", for insert
func JoinNamedColumns(cols ...string) string {
	return strings.Join(cols, ",")
}

// ":foo,:bar", for insert
func JoinNamedValues(cols ...string) string {
	values := make([]string, len(cols))
	for i, col := range cols {
		values[i] = ":" + col
	}
	return strings.Join(values, ",")
}

// "foo=:foo AND bar=:bar" , for where condition
func JoinNamedColumnsValuesWithOperator(cmp SqlCompare, oper SqlOperator, cols ...string) string {
	conds := strings.Join(namedTableColumnsValues(cmp, cols...), fmt.Sprintf(" %s ", oper))
	if len(cols) == 0 || conds == "" {
		return "TRUE"
	}

	return conds
}

// "foo=:foo"
func NamedColumnValue(cmp SqlCompare, col string) string {
	switch cmp {
	case SqlCompareLike:
		return fmt.Sprintf(`%[1]s %[2]s concat("%%",:%[1]s,"%%")`, col, cmp)
	case SqlCompareIn:
		return fmt.Sprintf("%[1]s %[2]s (:%[1]s)", col, cmp)
	case SqlCompareLogicAnd:
		return fmt.Sprintf("%[1]s & :%[1]s != 0", col)
	default:
		return fmt.Sprintf("%[1]s %[2]s :%[1]s", col, cmp)
	}
}

// "foo=CONCAT(foo, "(1645009767661044992)")"
func RandomUpdateColumn(col string) string {
	return fmt.Sprintf("%[1]s=CONCAT(%[1]s, '(%[2]s)')", col, strconv.FormatInt(time.Now().UnixNano(), 10))
}

// ToArgs ...
func ToArgs(ctx context.Context, query string, arg interface{}) (string, []interface{}, error) {
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		logger := common.GetLogger(ctx)
		logger.WithError(err).Errorf("invalid query %s", query)
		return "", nil, common.Errorf(ctx, code.Code_InternalError__Unknown)
	}
	return query, args, nil
}

// []string{"foo=:foo",  "bar=:bar"}
func namedTableColumnsValues(cmp SqlCompare, cols ...string) []string {
	var namedCols []string
	for _, col := range cols {
		if col != "" {
			namedCols = append(namedCols, NamedColumnValue(cmp, col))
		}
	}
	return namedCols
}
