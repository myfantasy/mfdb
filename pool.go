package mfdb

import (
	"database/sql"
	"sync"
	"time"

	"github.com/myfantasy/mfe"

	log "github.com/sirupsen/logrus"
)

// Pool pool of connections
type Pool struct {
	name           string
	mutex          sync.RWMutex
	actualSettings string
}

// PoolItem - one connection to Database
type PoolItem struct {
	Name           string
	DriverName     string
	DataSourceName string
	DB             *sql.DB
	Data           mfe.Variant
}

//LoadParams - Load params to Pool
func (p *Pool) LoadParams(params string) (err error) {
	log.Debug("LoadParams start. pool: " + p.name)

	p.mutex.Lock()
	defer p.mutex.Unlock()

	log.Debug("LoadParams lock. pool: " + p.name)

	v, e := mfe.VariantNewFromJSON(params)

	if e != nil {
		log.Debug("LoadParams fail convert from Json. pool: " + p.name)
		return e
	}

	if v.IsSV() {
		log.Debug("LoadParams slice_load. pool: " + p.name)
	} else {
		log.Debug("LoadParams one_load. pool: " + p.name)
	}

	return nil
}

//PoolItemCreate Создание элемента пула
func PoolItemCreate(v mfe.Variant) (pi PoolItem, err error) {
	pi = PoolItem{}

	pi.DriverName = v.GE("driver_name").Str()

	log.Debug("PoolItemCreate driver_name. : " + pi.DriverName)

	pi.DataSourceName = v.GE("data_source_name").Str()

	log.Debug("PoolItemCreate data_source_name. : " + pi.DataSourceName)

	db, err := sql.Open(pi.DriverName, pi.DataSourceName)
	if err != nil {
		log.Debug("PoolItemCreate open connection. Error: " + err)
		return pi, nil
	}

	pi.DB = db

	ps := v.GE("pool_size")
	mps := v.GE("min_pool_size")
	lt := v.GE("pool_life_time_connection")

	log.Debug("PoolItemCreate pool_size. : " + ps.String())
	log.Debug("PoolItemCreate min_pool_size. : " + mps.String())
	log.Debug("PoolItemCreate pool_life_time_connection. : " + lt.String())

	if ps.IsDecimal() {
		pi.DB.SetMaxOpenConns(int(ps.Dec().IntPart()))
	}
	if mps.IsDecimal() {
		pi.DB.SetMaxIdleConns(int(mps.Dec().IntPart()))
	}
	if lt.IsDecimal() {
		pi.DB.SetConnMaxLifetime(time.Duration(time.Duration(lt.Dec().IntPart()) * time.Minute))
	}

	return pi, nil
}

//Load params
//Reload Connections
//Select Connection

type i interface {
	abc(i int) (j int)
}
