package rawdb

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// cleanerRecheckInterval is the frequency to check the key-value database for
	// chain progression that might permit new blocks to be removed from the consensus datastore
	cleanerRecheckInterval = time.Minute

	// cleanerBatchLimit is the maximum number of blocks to clean in one batch
	// before doing an fsync and deleting it from the key-value store.
	cleanerBatchLimit = 30000
)

const (
	deleteHeadersPgStr = "DELETE FROM eth.headers WHERE height BETWEEN $1 AND $2"
	getTailHeightPgStr = "SELECT height FROM eth.headers WHERE height > 0 ORDER BY height ASC LIMIT 1"
	getHeadHeightPgStr = "SELECT height FROM eth.headers ORDER BY height DESC LIMIT 1"
)

type cleaner struct {
	db     *sqlx.DB
	tail uint64
}

func NewDBCleaner(store ethdb.KeyValueStore) (*cleaner, error) {
	db := store.ExposeDB()
	pgdb, ok := db.(*sqlx.DB)
	if !ok {
		return nil, fmt.Errorf("expected underlying db of type %T got %T", &sqlx.DB{}, db)
	}
	var tailHeight uint64
	if err := pgdb.Get(&tailHeight, getTailHeightPgStr); err != nil {
		if err == sql.ErrNoRows {
			return &cleaner{
				db:      pgdb,
				tail: 1,
			}, nil
		}
		return nil, err
	}
	return &cleaner{
		db:      pgdb,
		tail: tailHeight,
	}, nil
}

func (c *cleaner) clean() {
	log.Info("Begining background consensus db cleaning routine")
	t := time.NewTicker(cleanerRecheckInterval)
	for {
		// TODO: enable smooth shutdown using quit channel
		select {
		case <- t.C:
			log.Info("Cleaner: looking for stale records")
			var headHeight uint64
			if err := c.db.Get(&headHeight, getHeadHeightPgStr); err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				log.Error("Cleaner unable to retrieve head height")
				continue
			}
			rangeEnd := c.tail + cleanerBatchLimit
			if headHeight <= params.ImmutabilityThreshold || rangeEnd > headHeight - params.ImmutabilityThreshold {
				continue
			}
			log.Info("Cleaner removing records between", "start", c.tail, "end", rangeEnd)
			_, err := c.db.Exec(deleteHeadersPgStr, c.tail, rangeEnd)
			if err != nil {
				log.Error("Cleaner unable to remove data", "error", err)
			}
			c.tail = rangeEnd + 1
		}
	}
}
