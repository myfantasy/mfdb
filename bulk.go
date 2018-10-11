package mfdb

import "github.com/myfantasy/mfe"

// BulkField Поля для копирования
type BulkField struct {
	Name string
	Type string
}

// SBulkField Slise of BulkField
type SBulkField struct {
	SBF []BulkField
}

// Columns Get Columns List
func (sbf SBulkField) Columns() (columns []string) {
	for _, bf := range sbf.SBF {
		columns = append(columns, bf.Name)
	}

	return columns
}

// Values Get Values List
func (sbf SBulkField) Values(v mfe.Variant) (args []interface{}) {
	for _, bf := range sbf.SBF {
		args = append(args, v.GE(bf.Name).ConvertToTypeOrNil(bf.Type))
	}

	return args
}

// BulkFieldCreate Create BulkField from Variant
func BulkFieldCreate(v mfe.Variant) (bf BulkField) {
	bf.Name = v.GE("name").Str()
	bf.Type = v.GE("type").Str()

	return bf
}

// SBulkFieldCreate Create SBulkField from Variant
func SBulkFieldCreate(v *mfe.Variant) (sbf SBulkField) {
	for _, vi := range v.SV() {
		sbf.SBF = append(sbf.SBF, BulkFieldCreate(vi))
	}

	return sbf
}
