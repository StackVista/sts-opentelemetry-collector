package convert

import "encoding/binary"

func ToUint64(arr []byte) uint64 {
	if len(arr) < 8 {
		return 0
	}

	return binary.BigEndian.Uint64(arr[len(arr)-8:])
}
