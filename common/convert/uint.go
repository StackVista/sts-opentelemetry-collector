package convert

import "encoding/binary"

// ConvertUint64ToByteArr converts a uint64 to a byte array. If the array is longer than 8 bytes, only the last 8 bytes are used.
func ConvertToUint64(arr []byte) uint64 {
	if len(arr) < 8 {
		return 0
	}

	return binary.BigEndian.Uint64(arr[len(arr)-8:])
}
