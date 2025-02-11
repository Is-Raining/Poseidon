package dao

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/code"
	"git.woa.com/yt-industry-ai/poseidon/api/protobuf-spec/types"
	"git.woa.com/yt-industry-ai/poseidon/pkg/common"
	"github.com/jmoiron/sqlx"
)

// DatabaseExecuteTimeout ...
var (
	DatabaseExecuteTimeout = time.Minute
)

// MAX_LIMIT ...
const (
	MAX_LIMIT = 200
)

// FilterCond condition args for filter
type FilterCond = map[string][]interface{}

// WithTimeout ...
func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// CheckOffsetLimit ...
func CheckOffsetLimit(ctx context.Context, offset, limit int32, maxLimit int) error {
	if offset < 0 {
		return common.ParameterTooSmall(ctx, "Offset", 0)
	}
	if limit < 1 {
		return common.ParameterTooSmall(ctx, "Limit", 1)
	}
	if limit > int32(maxLimit) {
		return common.ParameterTooLarge(ctx, "Limit", maxLimit)
	}
	return nil
}

// FormatFullCondNames ...
func FormatFullCondNames(
	tableName string,
	conds []string,
	args map[string]interface{},
) ([]string, map[string]interface{}) {
	var fullConds []string
	fullArgs := make(map[string]interface{})
	for i := range conds {
		if !strings.Contains(conds[i], ".") {
			fullConds = append(fullConds, fmt.Sprintf("%s.%s", tableName, conds[i]))
		} else {
			fullConds = append(fullConds, conds[i])
		}
	}

	for k, v := range args {
		if !strings.Contains(k, ".") {
			fullKey := fmt.Sprintf("%s.%s", tableName, k)
			fullArgs[fullKey] = v
		} else {
			fullArgs[k] = v
		}
	}

	return fullConds, fullArgs
}

// ProjectColumCondition ...
func ProjectColumCondition(ctx context.Context) (cond string, args []interface{}) {
	return `project_id in(?,?)`, []interface{}{common.GetProjectId(ctx), common.PUBLIC_PROJ}
}

// for test
func GetTestDBOrDie(rootDir string) *sqlx.DB {
	//cfgFile := path.Join(rootDir, "conf_test/dao.yaml")
	//config := mysql_.NewConfig(mysql_.WithViper(viper_.GetViper(cfgFile, "database.mysql")))
	//
	//db, err := config.Complete().New(context.Background())
	//if err != nil {
	//	panic(err)
	//}
	//
	//if db == nil {
	//	panic("db is not enable")
	//}

	return nil
}

// for test
func GetTestContext() context.Context {
	return common.NewDefaultTestContext()
}

// TAG_SEP ...
const (
	TAG_SEP       = ","
	DESC    Order = true
	ASC     Order = false
)

// FilterType ...
type FilterType int

// FILTER
const (
	FILTER_EQ    FilterType = 1
	FILTER_IN    FilterType = 2
	FILTER_LIKE  FilterType = 3
	FILTER_RANGE FilterType = 4
)

// Order ...
type Order bool

// String ...
func (x Order) String() string {
	if x {
		return "DESC"
	} else {
		return "ASC"
	}
}

// EscapeLike ...
func EscapeLike(v string) string {
	str := strings.ReplaceAll(v, "_", `\_`)
	str = strings.ReplaceAll(str, "%", `\%`)
	return str
}

// ValueConv ...
type ValueConv func(context.Context, string) (interface{}, error)

// ParseFilters ...
func ParseFilters(
	ctx context.Context,
	filters []*types.Filter,
	filterMap map[string]FilterType,
	colMap map[string]string,
	valueConvMap map[string]ValueConv,
) (
	eqConds []string,
	inConds []string,
	likeConds []string,
	rangeConds []string,
	arg map[string]interface{},
	err error) {
	eqConds = []string{}
	inConds = []string{}
	likeConds = []string{}
	rangeConds = []string{}
	arg = map[string]interface{}{}

	unique := map[string]bool{}
	for _, filter := range filters {
		if len(filter.GetValues()) <= 0 {
			continue
		}
		k := filter.GetName()
		// unique filter name
		if _, ok := unique[k]; ok {
			continue
		}
		unique[k] = true
		// get filter type
		filterType, ok := filterMap[k]
		if !ok {
			return nil, nil, nil, nil, nil, common.Errorf(ctx, code.Code_InvalidParameter__UnsupportedFilter, k)
		}
		// get col
		col, ok := colMap[k]
		if !ok {
			return nil, nil, nil, nil, nil, common.Errorf(ctx, code.Code_InvalidParameter__UnsupportedFilter, k)
		}
		conv, shouldConv := valueConvMap[k]
		// collect values
		vs := []interface{}{}
		for _, v := range filter.GetValues() {
			if shouldConv {
				cv, err := conv(ctx, v)
				if err != nil {
					return nil, nil, nil, nil, nil, err
				}
				vs = append(vs, cv)
			} else {
				vs = append(vs, v)
			}
		}
		switch filterType {
		case FILTER_EQ:
			eqConds = append(eqConds, col)
			arg[col] = vs[0]
		case FILTER_IN:
			inConds = append(inConds, col)
			arg[col] = vs
		case FILTER_LIKE:
			likeConds = append(likeConds, col)
			arg[col] = vs[0]
		case FILTER_RANGE:
			if len(vs) != 2 {
				err := common.Errorf(ctx, code.Code_InvalidParameterValue__InvalidFilterValue, k, filter.GetValues())
				return nil, nil, nil, nil, nil, err
			}
			rangeConds = append(rangeConds, col)
			arg[col] = vs
		default:
			return nil, nil, nil, nil, nil, common.Errorf(ctx, code.Code_InvalidParameter__UnsupportedFilter, k)
		}
	}
	return eqConds, inConds, likeConds, rangeConds, arg, nil
}
