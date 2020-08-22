package decoder

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type decoder struct {
	Data   []byte
	Offset uint32
}

func (d *decoder) readNextBytes(octlen uint32) []byte {
	offset := d.Offset
	d.Offset += octlen
	return d.Data[offset:d.Offset]
}

func (d *decoder) getRemaining() uint32 {
	return uint32(len(d.Data)) - d.Offset
}

func (d *decoder) decodeU8() uint8 {
	return d.readNextBytes(1)[0]
}

func (d *decoder) decodeBool() bool {
	return d.readNextBytes(1)[0] != 0
}

func (d *decoder) decodeU32() (uint32, error) {
	var re uint32
	buffer := bytes.NewBuffer(d.readNextBytes(4))
	err := binary.Read(buffer, binary.LittleEndian, &re)
	return re, err
}

func (d *decoder) decodeU64() (uint64, error) {
	var re uint64
	buffer := bytes.NewBuffer(d.readNextBytes(8))
	err := binary.Read(buffer, binary.LittleEndian, &re)
	return re, err
}

func (d *decoder) decodeString() (string, error) {
	strLen, err := d.decodeU32()
	if err != nil {
		return "", err
	}
	offset := d.Offset
	d.Offset += strLen
	return string(d.Data[offset:d.Offset]), err
}

func (d *decoder) decodeTime() (s uint32, ns uint32, err error) {
	s, err = d.decodeU32()
	if err != nil {
		return
	}
	ns, err = d.decodeU32()
	return
}

func (d *decoder) decodeStart(v int) (structV uint8, structLen uint32, structEnd uint32, err error) {
	structV = d.decodeU8()
	structCompat := d.decodeU8()
	if v < int(structCompat) {
		err = errors.New("DECODE_ERR_OLDVERSION")
		return
	}
	structLen, err = d.decodeU32()
	if err != nil {
		return
	}
	if structLen > d.getRemaining() {
		err = errors.New("DECODE_ERR_PAST")
		return
	}
	structEnd = d.Offset + structLen
	return
}

func (d *decoder) decodeStartLegacyCompatLen(v, compactV, lenv uint32) (structV uint8, structEnd uint32, err error) {
	var structLen uint32

	cv := d.decodeU8()
	if err != nil {
		return
	}
	structV = cv
	if uint32(cv) >= compactV {
		sCompact := d.decodeU8()
		if v < uint32(sCompact) {
			err = errors.New("DECODE_ERR_OLDVERSION")
			return
		}
	}
	structEnd = 0
	if uint32(cv) >= lenv {
		structLen, err = d.decodeU32()
		if err != nil {
			return
		}
		if structLen > d.getRemaining() {
			err = errors.New("DECODE_ERR_PAST")
			return
		}
		structEnd = d.Offset + structLen
	}
	return
}

func (d *decoder) decodeFinish(structEnd uint32) error {
	if structEnd > 0 {
		if d.Offset > structEnd {
			return errors.New("DECODE_ERR_PAST")
		}
	}
	return nil
}

func DecodeAccessKey(data []byte) (string, error) {
	d := &decoder{
		Data: data,
	}
	return d.decodeString()
}
