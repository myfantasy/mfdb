package mfdb

import (
	"database/sql"
	"errors"
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
	items          []PoolItem
}

// PoolItem - one connection to Database
type PoolItem struct {
	Hash               string
	Name               string
	DriverName         string
	DataSourceName     string
	DB                 *sql.DB
	Data               mfe.Variant
	SetMaxOpenConns    int
	SetMaxIdleConns    int
	SetConnMaxLifetime time.Duration
	Priority           int
	Init               bool
}

// ConnectionGet get connection from pool
func (p *Pool) ConnectionGet() (pi PoolItem, err error) {
	if p.items == nil {
		return pi, errors.New("Connection Not Found")
	}

	b := false

	for _, pil := range p.items {
		if !b && pil.Init {
			pi = pil
			b = true
		}

		if pi.Priority < pil.Priority {
			pi = pil
		}
	}

	if !b {
		return pi, errors.New("Connection Not Found")
	}
	return pi, nil
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

	var items []PoolItem

	if v.IsSV() {
		log.Debug("LoadParams slice_load. pool: " + p.name)

		for _, vi := range v.SV() {
			pic, err := PoolItemCreate(vi)

			if err != nil {
				return err
			}

			items = append(items, pic)
		}

	} else {
		log.Debug("LoadParams one_load. pool: " + p.name)

		pic, err := PoolItemCreate(v)

		if err != nil {
			return err
		}

		items = append(items, pic)
	}

	if p.items == nil {
		p.items = items
	} else {
		var itemsR []PoolItem
		var itemsS []PoolItem
		var itemsI []PoolItem

		for _, pi := range items {
			b := false
			for _, pio := range p.items {
				if pio.Hash == pi.Hash {
					b = true
					itemsS = append(itemsS, pio)
				}
			}
			if !b {
				itemsS = append(itemsS, pi)
			}
		}

		for _, pi := range items {
			b := false
			for _, pio := range p.items {
				if pio.Hash == pi.Hash {
					b = true
					itemsS = append(itemsS, pio)
					if !pio.Init {
						itemsI = append(itemsI, pio)
					}
				}
			}
			if !b {
				itemsI = append(itemsI, pi)
				itemsS = append(itemsS, pi)
			}
		}

		for _, pio := range p.items {

			b := false
			for _, pi := range items {
				if pio.Hash == pi.Hash {
					b = true
				}
			}
			if !b {
				itemsR = append(itemsR, pio)
			}
		}

		p.items = itemsS

		for _, pii := range itemsR {
			er := pii.Reconnect()
			if er != nil {
				log.Debug("Fail to close Connection. : " + er.Error())
			}
		}

		go func() {
			for _, pi := range itemsR {
				er := pi.DB.Close()
				if er != nil {
					log.Debug("Fail to close Connection. : " + er.Error())
				}
			}
		}()
	}

	return nil
}

//PoolItemCreate Create pool item
func PoolItemCreate(v mfe.Variant) (pi PoolItem, err error) {
	pi = PoolItem{}

	pi.Data = v
	pi.Hash = v.String()

	pi.DriverName = v.GE("driver_name").Str()

	log.Debug("PoolItemCreate driver_name. : " + pi.DriverName)

	pi.DataSourceName = v.GE("data_source_name").Str()

	log.Debug("PoolItemCreate data_source_name. : " + pi.DataSourceName)

	ps := v.GE("pool_size")
	mps := v.GE("min_pool_size")
	lt := v.GE("pool_life_time_connection")
	pr := v.GE("priority")

	log.Debug("PoolItemCreate pool_size. : " + ps.String())
	log.Debug("PoolItemCreate min_pool_size. : " + mps.String())
	log.Debug("PoolItemCreate pool_life_time_connection. : " + lt.String())

	pi.SetMaxOpenConns = -1
	pi.SetMaxIdleConns = -1
	pi.SetConnMaxLifetime = -1

	pi.Priority = 0

	if ps.IsDecimal() {
		pi.SetMaxOpenConns = int(ps.Dec().IntPart())
	}
	if mps.IsDecimal() {
		pi.SetMaxIdleConns = int(mps.Dec().IntPart())
	}
	if lt.IsDecimal() {
		pi.SetConnMaxLifetime = time.Duration(time.Duration(lt.Dec().IntPart()) * time.Minute)
	}

	if pr.IsDecimal() {
		pi.Priority = int(pr.Dec().IntPart())
	}

	return pi, nil
}

// Reconnect to pool item
func (pi *PoolItem) Reconnect() (err error) {

	db, err := sql.Open(pi.DriverName, pi.DataSourceName)
	if err != nil {
		log.Debug("PoolItemCreate open connection. Error: " + err.Error())
		return err
	}

	pi.DB = db

	pi.Init = true

	if pi.SetMaxOpenConns >= 0 {
		pi.DB.SetMaxOpenConns(pi.SetMaxOpenConns)
	}
	if pi.SetMaxIdleConns >= 0 {
		pi.DB.SetMaxIdleConns(pi.SetMaxIdleConns)
	}
	if pi.SetConnMaxLifetime >= 0 {
		pi.DB.SetConnMaxLifetime(pi.SetConnMaxLifetime)
	}

	return nil
}
