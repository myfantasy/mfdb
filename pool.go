package mfdb

import (
	"database/sql"
	"sync"

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

func poolItemCreate(v mfe.Variant) (pi PoolItem, err error) {
	pi = PoolItem{}

	return pi, nil
}

//Load params
//Reload Connections
//Select Connection

type i interface {
	abc(i int) (j int)
}
