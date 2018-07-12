package mfdb

import (
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
