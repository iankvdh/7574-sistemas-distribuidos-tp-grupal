package safeio

import "io"

func ReadAll(reader io.Reader, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	nread, err := io.ReadFull(reader, buf)
	if err != nil || uint32(nread) < size {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func WriteAll(writer io.Writer, msg []byte) error {
	pending := msg
	for len(pending) > 0 {
		n, err := writer.Write(pending)
		if err != nil {
			return err
		}
		pending = pending[n:]
	}
	return nil
}
