package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	values, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	reader := engine_util.NewCFIterator(cf, s.txn)
	return reader
}

func (s *StandAloneStorageReader) Close() {
	s.txn = nil
}
