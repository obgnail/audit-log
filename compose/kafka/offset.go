package kafka

import (
	"encoding/binary"
	"fmt"
	"github.com/etcd-io/bbolt"
	"github.com/obgnail/audit-log/config"
	"github.com/pingcap/errors"
)

var KafkaOffset *Offset

func InitKafkaOffset() error {
	var err error
	offsetStorePath := config.StringOrPanic("kafka_offset_store_path")
	KafkaOffset, err = NewOffset(offsetStorePath)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type Offset struct {
	store *store
}

var kafkaOffsetBucket = []byte("kafka")

func NewOffset(storePath string) (*Offset, error) {
	s, err := newStore(storePath, kafkaOffsetBucket)
	if err != nil {
		return nil, errors.Trace(err)
	}
	offset := &Offset{
		store: s,
	}
	return offset, nil
}

func (o *Offset) Get(topic string, partition int32) ([]byte, error) {
	key := buildKey(topic, partition)
	offsetByte, err := o.store.Get(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return offsetByte, nil
}

func (o *Offset) Put(topic string, partition int32, offset int64) error {
	raw := make([]byte, 8)
	v := uint64(offset)
	binary.LittleEndian.PutUint64(raw, v)
	key := buildKey(topic, partition)
	err := o.store.Put(key, raw)
	return errors.Trace(err)
}

func buildKey(topic string, partition int32) []byte {
	return []byte(fmt.Sprintf("%s-%d", topic, partition))
}

type store struct {
	db         *bbolt.DB
	bucketName []byte
}

func newStore(path string, bucketName []byte) (*store, error) {
	s := new(store)
	var err error
	s.db, err = bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = s.db.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			_, err := tx.CreateBucket(bucketName)
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.bucketName = bucketName
	return s, nil
}

func (s *store) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return errors.New("bucket not found")
		}
		value = b.Get(key)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return value, nil
}

func (s *store) Put(key []byte, value []byte) error {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return errors.New("bucket not found")
		}
		err := b.Put(key, value)
		return errors.Trace(err)
	})
	return errors.Trace(err)
}
