package standalone_storage

import (
	"errors"
	"sync"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *badger.DB

	state int // 0: stopped, 1: started

	mtx *sync.RWMutex
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//TODO:无法通过test 检查路径是否弄错了
	ops := badger.DefaultOptions
	ops.Dir = conf.DBPath
	ops.ValueDir = conf.StoreAddr
	db, _ := badger.Open(ops)
	model := StandAloneStorage{
		engine: db,
		state:  0,
		mtx:    &sync.RWMutex{},
	}
	return &model
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.state = 1
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.state = 0
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if s.state == 0 {
		return nil, errors.New("storage is stopped")
	}
	txn := s.engine.NewTransaction(false)
	defer txn.Discard()
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if s.state == 0 {
		return errors.New("storage is stopped")
	}
	txn := s.engine.NewTransaction(true)
	defer txn.Discard()
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := modify.Data.(storage.Delete)
			err := txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
			if err != nil {
				return err
			}
		}
	}
	return txn.Commit()
}
