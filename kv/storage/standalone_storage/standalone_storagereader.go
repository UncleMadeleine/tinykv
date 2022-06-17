package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

type StandAloneStorageReader struct {
	txn *badger.Txn
	w   *log.Logger
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	// s.w.Debugf("GetCF: %s %v", cf, key)
	values, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil {
		// s.w.Debug("GetCF error: ", err)
		return nil, nil
	}
	// s.w.Debugf("GetCF: %v", values)
	return values, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	reader := engine_util.NewCFIterator(cf, s.txn)
	return reader
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
	s.txn = nil
}
