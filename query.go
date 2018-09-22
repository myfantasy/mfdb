package mfdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/myfantasy/mfe"
	"github.com/shopspring/decimal"
)

// SF Format value to PG
func SF(i interface{}) (s string) {
	if i == nil {
		return "null"
	}

	switch i.(type) {
	case mfe.Variant:
		v := i.(mfe.Variant)
		if v.IsNull() {
			return "null"
		}
		if v.IsSimpleValue() {
			return SF(v.Value())
		}
		return SF(v.String())

	case bool:
		if i.(bool) {
			return "true"
		}
		return "false"
	case time.Time:
		return "'" + i.(time.Time).Format("20060102 150405.999999") + "'"
	case string:
		return "'" + strings.Replace(i.(string), "'", "''", -1) + "'"
	case float32:
		return fmt.Sprintf("%f", i)
	case float64:
		return fmt.Sprintf("%f", i)
	case decimal.Decimal:
		return i.(decimal.Decimal).String()
	case int, int64, int32, int16, int8:
		return fmt.Sprintf("%v", i)
	case []uint8:
		return string(i.([]uint8))
	}

	return SF(fmt.Sprintf("%v", i))
}

// SFMS Format value to MS
func SFMS(i interface{}) (s string) {
	if i == nil {
		return "null"
	}

	switch i.(type) {
	case mfe.Variant:
		v := i.(mfe.Variant)
		if v.IsNull() {
			return "null"
		}
		if v.IsSimpleValue() {
			return SFMS(v.Value())
		}
		return SFMS(v.String())

	case bool:
		if i.(bool) {
			return "1"
		}
		return "0"
	}
	return SF(i)
}

// Execute some query in db
func Execute(db *sql.DB, query string) (v mfe.Variant, e error) {
	svAll := make(mfe.SV, 0)

	r, e := db.Query(query)

	if e != nil {
		return mfe.VariantNewNull(), e
	}

	for r.NextResultSet() {
		svDataRes := make(mfe.SV, 0)

		cols, _ := r.Columns()
		ct, _ := r.ColumnTypes()

		vals := make([]interface{}, len(cols))

		for i := range vals {
			vals[i] = new(interface{})
		}

		for r.Next() {
			vm := mfe.VMap{}

			er := r.Scan(vals...)
			if er != nil {
				return mfe.VariantNewNull(), e
			}
			for i := range vals {
				vv := mfe.VariantNew(*(vals[i].(*interface{})))
				dtn := ct[i].DatabaseTypeName()
				if dtn == "NUMERIC" && !vv.IsNull() {
					vm[ct[i].Name()] = vv.ToDecimal()
				} else {
					vm[ct[i].Name()] = vv
				}

			}

			svDataRes = append(svDataRes, mfe.VariantNew(vm))
		}

		svAll = append(svAll, mfe.VariantNew(svDataRes))
	}

	return mfe.VariantNew(svAll), nil
}

// ExecuteWithPrepare some query in db with prepare query
func ExecuteWithPrepare(ctx context.Context, db *sql.DB, query string, prepareQuery string) (v mfe.Variant, e error) {
	svAll := make(mfe.SV, 0)

	c, ec := db.Conn(ctx)
	if ec != nil {
		return mfe.VariantNewNull(), ec
	}
	defer c.Close()

	if prepareQuery != "" {
		_, ep := c.QueryContext(ctx, prepareQuery)
		if ec != nil {
			return mfe.VariantNewNull(), ep
		}
	}

	r, e := c.QueryContext(ctx, prepareQuery)

	if e != nil {
		return mfe.VariantNewNull(), e
	}

	for r.NextResultSet() {
		svDataRes := make(mfe.SV, 0)

		cols, _ := r.Columns()
		ct, _ := r.ColumnTypes()

		vals := make([]interface{}, len(cols))

		for i := range vals {
			vals[i] = new(interface{})
		}

		for r.Next() {
			vm := mfe.VMap{}

			er := r.Scan(vals...)
			if er != nil {
				return mfe.VariantNewNull(), e
			}
			for i := range vals {
				vv := mfe.VariantNew(*(vals[i].(*interface{})))
				dtn := ct[i].DatabaseTypeName()
				if dtn == "NUMERIC" && !vv.IsNull() {
					vm[ct[i].Name()] = vv.ToDecimal()
				} else {
					vm[ct[i].Name()] = vv
				}

			}

			svDataRes = append(svDataRes, mfe.VariantNew(vm))
		}

		svAll = append(svAll, mfe.VariantNew(svDataRes))
	}

	return mfe.VariantNew(svAll), nil
}

// Execute query in Pull
func (p *Pool) Execute(ctx context.Context, query string) (v mfe.Variant, e error) {
	pi, er := p.ConnectionGet()
	if er != nil {
		return mfe.VariantNewNull(), er
	}

	db, err := pi.GDB()
	if err != nil {
		return mfe.VariantNewNull(), err
	}

	return ExecuteWithPrepare(ctx, db, query, pi.ContextPrepare)
}

// Execute query in ConnectionsCollections
func (cc *ConnectionsCollections) Execute(ctx context.Context, dbName string, query string) (v mfe.Variant, e error) {
	p, t := cc.Pools[dbName]
	if !t {
		return mfe.VariantNewNull(), errors.New("dbName [" + dbName + "] not found")
	}

	return p.Execute(ctx, query)
}
