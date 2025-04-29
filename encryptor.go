package consistent_hash

import (
	"math"

	"github.com/spaolacci/murmur3"
)

type Encryptor interface {
	Encrypt(data string) (int32, error)
}

type murmur3Encryptor struct{}

func NewMurmur3Encryptor() Encryptor {
	return &murmur3Encryptor{}
}

func (m *murmur3Encryptor) Encrypt(data string) (int32, error) {
	hasher := murmur3.New32()
	_, _ = hasher.Write([]byte(data))
	hash := hasher.Sum32() % math.MaxInt32
	return int32(hash), nil
}
