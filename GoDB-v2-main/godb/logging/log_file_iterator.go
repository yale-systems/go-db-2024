package logging

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"mit.edu/dsg/godb/common"
)

type LogFileIterator struct {
	file   *os.File
	reader *bufio.Reader

	offset int64

	recordBuf  [MaxLogRecordSize]byte
	currentRec LogRecord
	err        error
}

func NewLogFileIterator(path string, startLSN common.LSN) (*LogFileIterator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Seek to the start LSN before creating the buffered reader
	if _, err := f.Seek(int64(startLSN), io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}

	return &LogFileIterator{
		file:   f,
		reader: bufio.NewReader(f),
		offset: int64(startLSN),
	}, nil
}

func (iter *LogFileIterator) Next() bool {
	if iter.err != nil {
		return false
	}

	if !iter.currentRec.IsNil() {
		iter.offset += int64(iter.currentRec.Size())
	}

	sizeBytes, err := iter.reader.Peek(2)
	if err != nil {
		// Handle cleanly if we hit EOF exactly between records
		if err != io.EOF {
			iter.err = err
		}
		return false
	}

	recordLen := int(binary.LittleEndian.Uint16(sizeBytes))
	if recordLen == 0 {
		// Clean EOF (pre-allocated zeros usually found at end of log files)
		return false
	}

	if recordLen > MaxLogRecordSize {
		iter.err = errors.New("log record corrupted: length exceeds maximum limit")
		return false
	}

	_, err = io.ReadFull(iter.reader, iter.recordBuf[:recordLen])
	if err != nil {
		if err == io.EOF {
			iter.err = io.ErrUnexpectedEOF // EOF in middle of record is an error
		} else {
			iter.err = err
		}
		return false
	}

	iter.currentRec, err = AsVerifiedLogRecord(iter.recordBuf[:recordLen])
	if err != nil {
		iter.err = err
		return false
	}
	return true

}

func (iter *LogFileIterator) CurrentRecord() LogRecord {
	return iter.currentRec
}

func (iter *LogFileIterator) CurrentLSN() common.LSN {
	return common.LSN(iter.offset)
}

func (iter *LogFileIterator) Error() error {
	if iter.err == io.EOF {
		return nil
	}
	return iter.err
}

func (iter *LogFileIterator) Close() error {
	return iter.file.Close()
}
