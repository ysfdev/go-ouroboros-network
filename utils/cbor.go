package utils

import (
	"github.com/fxamacker/cbor/v2"
)

func CborEncode(data interface{}) ([]byte, error) {
	dataBytes, err := cbor.Marshal(data)
	return dataBytes, err
}

func CborDecode(dataBytes []byte, dest interface{}) error {
	decOptions := cbor.DecOptions{
		MapKeyByteString: cbor.MapKeyByteStringWrap,
	}
	decMode, err := decOptions.DecMode()
	if err != nil {
		return err
	}
	err = decMode.Unmarshal(dataBytes, dest)
	return err
}
