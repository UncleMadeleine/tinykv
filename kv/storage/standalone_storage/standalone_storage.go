package standalone_storage

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/pingcap-incubator/tinykv/log"

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
	lg  loggist
}

type loggist struct {
	state int
	w     *log.Logger
}

func (m *loggist) Printf(format string, v ...interface{}) {
	if m.state == 0 {
		return
	}
	m.w.Debugf(format, v...)
}

func Newlg(st int) *loggist {
	writer2 := os.Stdout
	writer3, _ := os.OpenFile("../storage/standalone_storage/logFile/log.log", os.O_WRONLY|os.O_CREATE, 0755)
	lg := loggist{
		state: st,
		w:     log.NewLogger(io.MultiWriter(writer2, writer3), "Project1:"),
	}
	lg.w.SetLevel(log.LOG_LEVEL_DEBUG)
	lg.w.SetHighlighting(true)
	return &lg
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	ops := badger.DefaultOptions
	ops.Dir = conf.DBPath
	ops.ValueDir = conf.DBPath
	db, _ := badger.Open(ops)
	model := StandAloneStorage{
		engine: db,
		state:  0,
		mtx:    &sync.RWMutex{},
		lg:     *Newlg(0), //如果需要输出日志，置1
	}
	return &model
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.lg.Printf("start storage")
	if s.state == 1 {
		return errors.New("DB has been already started")
	}
	s.state = 1
	s.lg.Printf("start successfully")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.lg.Printf("stop storage")
	if s.state == 0 {
		return errors.New("DB has been already stopped")
	}
	s.state = 0
	s.lg.Printf("stop successfully")
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	s.lg.Printf("get reader")
	if s.state == 0 {
		return nil, errors.New("storage is stopped")
	}
	txn := s.engine.NewTransaction(false)
	// defer txn.Discard()
	s.lg.Printf("read successfully")
	res := &StandAloneStorageReader{txn: txn, w: log.NewLogger(os.Stdout, "Project1:")}
	res.w.SetLevel(log.LOG_LEVEL_DEBUG)
	res.w.SetHighlighting(true)
	return res, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	s.lg.Printf("write")
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
				s.lg.Printf("err: The key to delete is not found!", err)
				return err
			}
		}
	}

	err := txn.Commit()
	if err != nil {
		s.lg.Printf("write failed" + err.Error())
		return err
	}
	s.lg.Printf("written successfully")
	return nil
}
