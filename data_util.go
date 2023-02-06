package mitkv

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

// Int64ToBytes Float64转byte
func Int64ToBytes(bits int64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(bits))
	return bytes
}

// BytesToInt64 byte转Float64
func BytesToInt64(bytes []byte) int64 {
	bits := binary.LittleEndian.Uint64(bytes)
	return int64(bits)
}

// Int32ToBytes Float64 to byte
func Int32ToBytes(bits int32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(bits))
	return bytes
}

// BytesToInt32 byte转Float64
func BytesToInt32(bytes []byte) int32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return int32(bits)
}

// Value2Str interface val to str
func Value2Str(v interface{}) string {
	switch tv := v.(type) {
	case nil:
		return ""
	case bool:
		if tv {
			return "true"
		}
		return "false"
	case string:
		return tv
	case []byte:
		return string(tv)
	case error:
		return tv.Error()
	case int:
		return strconv.Itoa(tv)
	case int16:
		return strconv.FormatInt(int64(tv), 10)
	case int32:
		return strconv.FormatInt(int64(tv), 10)
	case int64:
		return strconv.FormatInt(int64(tv), 10)
	case uint:
		return strconv.FormatUint(uint64(tv), 10)
	case uint16:
		return strconv.FormatUint(uint64(tv), 10)
	case uint32:
		return strconv.FormatUint(uint64(tv), 10)
	case uint64:
		return strconv.FormatUint(uint64(tv), 10)
	case float32:
		return strconv.FormatFloat(float64(tv), 'f', 3, 32)
	case float64:
		return strconv.FormatFloat(float64(tv), 'f', 3, 32)
	default:
		return fmt.Sprint(v)
	}
}
