package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	log.Info("RawGet Start")
	defer func() {
		log.Info("RawGet End")
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	res, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	rep := &kvrpcpb.RawGetResponse{
		Value:    res,
		NotFound: res == nil,
		Error:    "",
	}
	log.Debug("RawGet:", rep.Value, "successfully")
	return rep, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	log.Info("RawPut Start")
	defer func() {
		log.Info("RawPut End")
	}()
	m := storage.Modify{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{m})
	if err != nil {
		log.Debug("RawPut Error:", err)
		return nil, err
	}
	rep := &kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}
	return rep, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	log.Info("RawDelete Start")
	defer func() {
		log.Info("RawDelete End")
	}()
	m := storage.Modify{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{m})
	log.Debug("delete err:", err)
	if err != nil {
		log.Debug("RawDelete Error:", err)
		return nil, err
	}
	log.Info("RawDelete successfully")
	rep := &kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}
	return rep, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	log.Info("RawScan Start")
	defer func() {
		log.Info("RawScan End")
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Debug("1:", err)
		return nil, err
	}
	res := reader.IterCF(req.Cf)
	defer res.Close()

	res.Seek(req.StartKey)
	var cnt uint32 = 0
	rep := &kvrpcpb.RawScanResponse{
		Kvs: make([]*kvrpcpb.KvPair, 0),
	}
	for res.Valid() {
		v, _ := res.Item().Value()
		log.Debug("RawScan:", "The Item is:k: ", res.Item().Key(), " v: ", v)
		rep.Kvs = append(rep.Kvs, &kvrpcpb.KvPair{
			Key:   res.Item().Key(),
			Value: v,
		})
		cnt++
		if cnt == req.Limit {
			break
		}
		res.Next()
	}

	return rep, nil
}
