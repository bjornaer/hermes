package disk

import (
	"encoding/binary"
	"fmt"
	"time"
)

// 2+2+30+93+8+2 = 137
const pairSize = 137
const maxKeyLength = 30
const maxValueLength = 93

type Pairs struct {
	KeyLen    uint16    // 2
	ValueLen  uint16    // 2
	Key       string    // 30
	Value     string    // 93 // serialize this on the client side, to enable more complex data
	Timestamp time.Time // 8
	TimeLen   uint16    // 2
}

func (p *Pairs) SetKey(key string) {
	p.Key = key
	p.KeyLen = uint16(len(key))
}

func (p *Pairs) SetValue(value string) {
	p.Value = value
	p.ValueLen = uint16(len(value))
}

func (p *Pairs) SetTime(t time.Time) {
	p.Timestamp = t
	p.TimeLen = 8
}

func (p *Pairs) Validate() error {
	if len(p.Key) > maxKeyLength {
		return fmt.Errorf("key length should not be more than 30, currently it is %d ", len(p.Key))
	}
	if len(p.Value) > maxValueLength {
		return fmt.Errorf("value length should not be more than 93, currently it is %d", len(p.Value))
	}
	return nil
}

func NewPair(key string, value string) *Pairs {
	pair := new(Pairs)
	pair.SetKey(key)
	pair.SetValue(value)
	pair.SetTime(time.Now())
	return pair
}

func NewPairWithTime(key string, value string, t time.Time) *Pairs {
	pair := new(Pairs)
	pair.SetKey(key)
	pair.SetValue(value)
	pair.SetTime(t)
	return pair
}

func ConvertPairsToBytes(pair *Pairs) []byte {
	pairByte := make([]byte, pairSize)
	var pairOffset uint16
	pairOffset = 0
	copy(pairByte[pairOffset:], uint16ToBytes(pair.KeyLen))
	pairOffset += 2
	copy(pairByte[pairOffset:], uint16ToBytes(pair.ValueLen))
	pairOffset += 2
	copy(pairByte[pairOffset:], uint16ToBytes(pair.TimeLen))
	pairOffset += 2
	keyByte := []byte(pair.Key)
	copy(pairByte[pairOffset:], keyByte[:pair.KeyLen])
	pairOffset += pair.KeyLen
	valueByte := []byte(pair.Value)
	copy(pairByte[pairOffset:], valueByte[:pair.ValueLen])
	pairOffset += pair.ValueLen
	timeByte := epochToBytes(pair.Timestamp.Unix())
	copy(pairByte[pairOffset:], timeByte[:pair.TimeLen])
	return pairByte
}

func ConvertBytesToPair(pairByte []byte) *Pairs {
	pair := new(Pairs)
	var pairOffset uint16
	pairOffset = 0
	//Read key length
	pair.KeyLen = uint16FromBytes(pairByte[pairOffset:])
	pairOffset += 2
	//Read value length
	pair.ValueLen = uint16FromBytes(pairByte[pairOffset:])
	pairOffset += 2
	//Read timestamp length
	pair.TimeLen = uint16FromBytes(pairByte[pairOffset:])
	pairOffset += 2
	pair.Key = string(pairByte[pairOffset : pairOffset+pair.KeyLen])
	pairOffset += pair.KeyLen
	pair.Value = string(pairByte[pairOffset : pairOffset+pair.ValueLen])
	pairOffset += pair.ValueLen
	pair.Timestamp = time.Unix(bytesToEpoch(pairByte[pairOffset:pairOffset+pair.TimeLen]), 0)
	return pair
}

func uint16FromBytes(b []byte) uint16 {
	i := uint16(binary.LittleEndian.Uint64(b))
	return i
}

func uint16ToBytes(value uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(value))
	return b
}

func epochToBytes(t int64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint32(out, uint32(t))
	return out
}

func bytesToEpoch(b []byte) int64 {
	i := int64(binary.LittleEndian.Uint64(b))
	return i
}
