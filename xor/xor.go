package xor

import (
	"crypto/md5"
	"encoding/binary"
	"hash"
)

func New() hash.Hash {
	return &digest{}
}

// XOR checksum digest the data into a md5 bytes, and XOR the md5 (16byte).
type digest struct {
	s [4]uint32
}

func (d *digest) Reset() {
	d.s = [4]uint32{0, 0, 0, 0}
}

func (d *digest) Size() int {
	return md5.Size
}

func (d *digest) BlockSize() int {
	return md5.BlockSize
}

func (d *digest) Sum(in []byte) []byte {
	var b16 [md5.Size]byte
	for i := range d.s {
		binary.LittleEndian.PutUint32(b16[i*4:(i+1)*4], d.s[i])
	}
	return append(in, b16[:]...)
}

func (d *digest) Write(p []byte) (int, error) {
	md5sum := md5.New()
	n, err := md5sum.Write(p)
	if err != nil {
		return n, err
	}
	data := md5sum.Sum(nil)
	for i := range d.s {
		u32 := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		d.s[i] = d.s[i] ^ u32
	}
	return n, nil
}
