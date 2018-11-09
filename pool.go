package mfdb

import (
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/myfantasy/mfe"
	log "github.com/sirupsen/logrus"
)

// ConnectionsCollections Pool Collections
type ConnectionsCollections struct {
	Pools map[string]*Pool
}

// ConnectionsCollectionsCreate create ConnectionsCollections
func ConnectionsCollectionsCreate() (cc ConnectionsCollections) {
	cc.Pools = make(map[string]*Pool)
	return cc
}

// Load load ConnectionsCollections from Variant
func (cc *ConnectionsCollections) Load(v *mfe.Variant) (err error) {

	if v.IsSV() {
		mfe.LogActionF("", "mfdb.ConnectionsCollections.Load", "IsSV")

		for _, vi := range v.SV() {
			err := cc.Load(&vi)
			if err != nil {
				return err
			}
		}

	} else {
		mfe.LogActionF("", "mfdb.ConnectionsCollections.Load", "IsSV else")

		name := v.GE("name").Str()
		pv := v.GE("params")
		rchDur := v.GE("recheck")
		rcoDur := v.GE("reconnection")
		rchd := 0 * time.Minute
		rcod := 0 * time.Minute

		if name == "" {
			mfe.LogInnerErrorF("no name", "mfdb.ConnectionsCollections.Load", "IsSV else")
			return errors.New("Not set connection name")
		}
		if pv.IsNull() {
			mfe.LogInnerErrorF("no connection params cc:"+name, "mfdb.ConnectionsCollections.Load", "IsSV else")
			return errors.New("Not set connection params")
		}
		if rchDur.IsDecimal() {
			mfe.LogActionF("ch: "+rchDur.String()+" cc:"+name, "mfdb.ConnectionsCollections.Load", "IsSV else")
			rchd = time.Millisecond * time.Duration(rchDur.Dec().IntPart())
		}
		if rcoDur.IsDecimal() {
			mfe.LogActionF("co: "+rcoDur.String()+" cc:"+name, "mfdb.ConnectionsCollections.Load", "IsSV else")
			rcod = time.Millisecond * time.Duration(rcoDur.Dec().IntPart())
		}

		mfe.LogActionF("params loaded cc:"+name, "mfdb.ConnectionsCollections.Load", "IsSV else")

		p, e := cc.Pools[name]
		if !e {
			mfe.LogActionF("cc:"+name, "mfdb.ConnectionsCollections.Load", "IsSV else new conection")

			pc := Pool{Name: name, RecheckDuration: rchd, ReconnectDuration: rcod}
			p = &pc
			cc.Pools[name] = p
			er := p.LoadParams(pv)

			if er == nil {
				p.RecheckConnections(true)
				p.InitRecheck()
			}

			return er
		}

		mfe.LogActionF("cc:"+name, "mfdb.ConnectionsCollections.Load", "IsSV else exists conection")
		return p.LoadParams(pv)
	}

	return nil
}

// Pool pool of connections
type Pool struct {
	Name              string
	Mutex             sync.RWMutex
	ActualSettings    string
	Items             []*PoolItem
	ReconnectDuration time.Duration
	DoRecheck         bool
	RecheckDuration   time.Duration
}

// PoolItem - one connection to Database
type PoolItem struct {
	Hash               string
	Name               string
	DriverName         string
	DataSource         string
	ContextPrepare     string
	DB                 *sql.DB
	Data               mfe.Variant
	SetMaxOpenConns    int
	SetMaxIdleConns    int
	SetConnMaxLifetime time.Duration
	Priority           int
	Init               bool
	LastTryReConnect   time.Time
}

// Close all connections of pull
func (p *Pool) Close() {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	p.DoRecheck = false

	for _, pil := range p.Items {
		pil.Close()
	}
}

// InitRecheck start recheck
func (p *Pool) InitRecheck() {
	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	if p.DoRecheck || p.RecheckDuration == 0 {
		return
	}

	p.DoRecheck = true
	go func() {
		for p.DoRecheck && p.RecheckDuration > 0 {
			p.Mutex.Lock()
			defer p.Mutex.Unlock()
			p.RecheckConnections(false)
			p.Mutex.Unlock()
			time.Sleep(p.RecheckDuration)
		}
	}()
}

// RecheckConnections check connections and close or reopen them
func (p *Pool) RecheckConnections(force bool) {

	if !p.DoRecheck && !force {
		return
	}

	for _, pil := range p.Items {
		if pil.Init {
			db, _ := pil.GDB()
			err := db.Ping()
			if err != nil {
				pil.Close()
			}
		}

		if !pil.Init {
			if time.Since(pil.LastTryReConnect) >= p.ReconnectDuration {
				pil.Reconnect()
			}
		}
	}
}

// ConnectionGet get connection from pool
func (p *Pool) ConnectionGet() (pi *PoolItem, err error) {
	if p.Items == nil {
		return pi, errors.New("Connection Not Found")
	}

	b := false

	for _, pil := range p.Items {

		if !b && pil.Init {
			pi = pil
			b = true
		}
		if b && pil.Init {
			if pi.Priority < pil.Priority {
				pi = pil
			}
		}
	}

	if !b {
		return pi, errors.New("Active Connection Not Found")
	}
	return pi, nil
}

// ConnectionGetAlt get connection from pool with alt conditions
func (p *Pool) ConnectionGetAlt(conditionUse func(pi *PoolItem) (b bool), conditionPriority func(pi *PoolItem) (i int64)) (pi *PoolItem, err error) {
	if p.Items == nil {
		return pi, errors.New("Connection Not Found")
	}

	b := false

	for _, pil := range p.Items {
		if !b && pil.Init && conditionUse(pil) {
			pi = pil
			b = true
		}

		if conditionPriority(pi) < conditionPriority(pil) {
			pi = pil
		}
	}

	if !b {
		return pi, errors.New("Connection Not Found")
	}
	return pi, nil
}

// GDB Get Database
func (pi *PoolItem) GDB() (DB *sql.DB, err error) {
	if pi.Init {
		return pi.DB, nil
	}
	return pi.DB, errors.New("Connection " + pi.Name + " Not Init")
}

// CheckConnection PingConnectionCheck
func (pi *PoolItem) CheckConnection() {
	db, _ := pi.GDB()
	err := db.Ping()
	if err != nil {
		log.Debug("CheckConnection. fail: " + pi.Name)
		pi.Close()
	}
}

//LoadParamsForomString - Load params to Pool
func (p *Pool) LoadParamsForomString(params string) (err error) {
	log.Debug("LoadParams lock. pool: " + p.Name)

	v, e := mfe.VariantNewFromJSON(params)

	if e != nil {
		log.Debug("LoadParams fail convert from Json. pool: " + p.Name)
		return e
	}
	return p.LoadParams(&v)
}

//LoadParams - Load params to Pool
func (p *Pool) LoadParams(v *mfe.Variant) (err error) {
	mfe.LogActionF("", "mfdb.Pool.LoadParams", "start")

	p.Mutex.Lock()
	defer p.Mutex.Unlock()

	var items []*PoolItem

	if v.IsSV() {
		mfe.LogActionF("LoadParams slice_load. pool: "+p.Name, "mfdb.Pool.LoadParams", "IsSV")

		for _, vi := range v.SV() {
			pic, err := PoolItemCreate(&vi)

			if err != nil {
				return err
			}

			items = append(items, &pic)
		}

	} else {
		mfe.LogActionF("LoadParams one_load. pool: "+p.Name, "mfdb.Pool.LoadParams", "IsSV else")

		pic, err := PoolItemCreate(v)

		if err != nil {
			return err
		}

		items = append(items, &pic)
	}

	if p.Items == nil {
		p.Items = items
		for _, pii := range p.Items {
			er := pii.Reconnect()
			if er != nil {
				mfe.LogExtErrorF(er.Error(), "mfdb.Pool.LoadParams", "pii.Reconnect")

			}
		}
	} else {
		var itemsR []*PoolItem
		var itemsS []*PoolItem
		var itemsI []*PoolItem

		for _, pi := range items {
			b := false
			for _, pio := range p.Items {
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
			for _, pio := range p.Items {
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

		for _, pio := range p.Items {

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

		p.Items = itemsS

		for _, pii := range itemsI {
			er := pii.Reconnect()
			if er != nil {
				mfe.LogExtErrorF(er.Error(), "mfdb.Pool.LoadParams", "pii.Reconnect")

			}
		}

		go func() {
			for _, pi := range itemsR {
				er := pi.Close()
				if er != nil {
					mfe.LogExtErrorF(er.Error(), "mfdb.Pool.LoadParams", "pi.Close")
				}
			}
		}()
	}

	return nil
}

//PoolItemCreate Create pool item
func PoolItemCreate(v *mfe.Variant) (pi PoolItem, err error) {
	mfe.LogActionF("", "mfdb.PoolItemCreate", "start")

	pi = PoolItem{}

	pi.Data = *v
	pi.Hash = v.String()
	pi.Name = v.GE("name").Str()

	pi.DriverName = v.GE("driver_name").Str()

	mfe.LogActionF("driver_name. : "+pi.DriverName, "mfdb.PoolItemCreate", "start")

	pi.DataSource = v.GE("data_source").Str()

	pi.ContextPrepare = v.GE("context_prepare").Str()

	mfe.LogActionF("data_source. : loaded", "mfdb.PoolItemCreate", "start")

	ps := v.GE("pool_size")
	mps := v.GE("min_pool_size")
	lt := v.GE("pool_life_time_connection")
	pr := v.GE("priority")

	mfe.LogActionF("pool_size. : "+ps.String(), "mfdb.PoolItemCreate", "start")
	mfe.LogActionF("min_pool_size. : "+mps.String(), "mfdb.PoolItemCreate", "start")
	mfe.LogActionF("pool_life_time_connection. : "+lt.String(), "mfdb.PoolItemCreate", "start")

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
		pi.SetConnMaxLifetime = time.Duration(time.Duration(lt.Dec().IntPart()) * time.Second)
	}

	if pr.IsDecimal() {
		pi.Priority = int(pr.Dec().IntPart())
	}

	return pi, nil
}

// Reconnect to pool item
func (pi *PoolItem) Reconnect() (err error) {
	mfe.LogActionF("", "mfdb.PoolItem.Reconnect", "start")

	pi.LastTryReConnect = time.Now()

	db, err := sql.Open(pi.DriverName, pi.DataSource)
	if err != nil {
		mfe.LogInnerErrorF(err.Error(), "mfdb.PoolItem.Reconnect", "sql.Open")
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

// Close to pool item
func (pi *PoolItem) Close() (err error) {
	mfe.LogActionF("name: "+pi.Name, "mfdb.PoolItem.Close", "start")
	pi.Init = false

	if !pi.Init {
		mfe.LogActionF("name: "+pi.Name, "mfdb.PoolItem.Close", "nothing to close")
		return nil
	}

	er := pi.DB.Close()
	if er != nil {
		mfe.LogExtErrorF("name: "+pi.Name+" "+err.Error(), "mfdb.PoolItem.Close", "DB.Close")
		return er
	}

	mfe.LogActionF("name: "+pi.Name, "mfdb.PoolItem.Close", "end")
	return nil
}
