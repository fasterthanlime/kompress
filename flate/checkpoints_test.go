package flate

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

func TestCheckpoints(t *testing.T) {
	gen := rand.New(rand.NewSource(0xfaadbeef))
	inputBuf := new(bytes.Buffer)

	var oldSeqs [][]byte

	for i := 0; i < 128; i++ {
		var seq []byte

		if gen.Intn(100) >= 80 {
			// re-use old seq
			seq = oldSeqs[gen.Intn(len(oldSeqs))]
		} else {
			seqLength := gen.Intn(48 * 1024)
			seq := make([]byte, seqLength)
			for j := 0; j < seqLength; j++ {
				seq[j] = byte(gen.Intn(255))
			}
			oldSeqs = append(oldSeqs, seq)
		}

		numRepetitions := gen.Intn(24)
		for j := 0; j < numRepetitions; j++ {
			_, err := inputBuf.Write(seq)
			assert.NoError(t, err)
		}
	}

	log.Printf("uncompressedBuf length: %s", humanize.IBytes(uint64(inputBuf.Len())))
	inputData := inputBuf.Bytes()

	compressedBuf := new(bytes.Buffer)
	w, err := NewWriter(compressedBuf, 9)
	assert.NoError(t, err)

	_, err = io.Copy(w, inputBuf)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	log.Printf("  compressedBuf length: %s", humanize.IBytes(uint64(compressedBuf.Len())))

	compressedData := compressedBuf.Bytes()
	decompressedBuf := new(bytes.Buffer)

	bytesReader := bytes.NewReader(compressedData)
	sr := NewSaverReader(bytesReader)
	const bufsize = 8 * 1024
	buf := make([]byte, bufsize)

	var checkpoint *Checkpoint
	var readBytes int64
	var threshold int64 = 256 * 1024
	for {
		n, err := sr.Read(buf)
		if n > 0 {
			readBytes += int64(n)

			_, err := decompressedBuf.Write(buf[:n])
			assert.NoError(t, err)
			if err != nil {
				t.FailNow()
			}
		}

		if err != nil {
			if err == io.EOF {
				// cool!
				log.Printf("got EOF!")
				break
			} else if err == ReadyToSaveError {
				var saveErr error
				checkpoint, saveErr = sr.Save()
				assert.NoError(t, saveErr)

				log.Printf("Made checkpoint at byte %d", checkpoint.Roffset)
				sr1 := sr.(*saverReader)

				var resumeErr error
				bytesReader = bytes.NewReader(compressedData)
				sr, resumeErr = checkpoint.Resume(bytesReader)
				assert.NoError(t, resumeErr)

				sr2 := sr.(*saverReader)
				log.Printf("rdPos: %v => %v", sr1.f.dict.rdPos, sr2.f.dict.rdPos)
				log.Printf("wrPos: %v => %v", sr1.f.dict.wrPos, sr2.f.dict.wrPos)
				log.Printf("full: %v => %v", sr1.f.dict.full, sr2.f.dict.full)
				log.Printf("len(hist): %v => %v", len(sr1.f.dict.hist), len(sr2.f.dict.hist))
			} else {
				offset, _ := bytesReader.Seek(0, io.SeekCurrent)
				log.Printf("Got unrecoverable error at byte %d/%d", offset, len(compressedData))
				assert.NoError(t, err)
				t.FailNow()
			}
		}

		if readBytes > threshold {
			sr.WantSave()
			readBytes = 0
		}
	}

	decompressedData := decompressedBuf.Bytes()
	assert.EqualValues(t, decompressedData, inputData)
}
