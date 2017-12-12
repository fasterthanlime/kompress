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

type checkpointTestSpec struct {
	numOps    int
	threshold int64
}

func TestCheckpointsSmall(t *testing.T) {
	runCheckpointTest(t, &checkpointTestSpec{
		numOps:    16,
		threshold: 128 * 1024,
	})
}

func TestCheckpointsMedium(t *testing.T) {
	runCheckpointTest(t, &checkpointTestSpec{
		numOps:    32,
		threshold: 512 * 1024,
	})
}

func TestCheckpointsLarge(t *testing.T) {
	runCheckpointTest(t, &checkpointTestSpec{
		numOps:    128,
		threshold: 1 * 1024 * 1024,
	})
}

func runCheckpointTest(t *testing.T, cts *checkpointTestSpec) {
	gen := rand.New(rand.NewSource(0xfaadbeef))
	inputBuf := new(bytes.Buffer)

	log.Printf("running test with %d ops, threshold %s", cts.numOps, humanize.IBytes(uint64(cts.threshold)))

	var oldSeqs [][]byte

	for i := 0; i < cts.numOps; i++ {
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

	inputData := inputBuf.Bytes()

	compressedBuf := new(bytes.Buffer)
	w, err := NewWriter(compressedBuf, 9)
	assert.NoError(t, err)

	_, err = io.Copy(w, inputBuf)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	log.Printf("compressed to %s / %s", humanize.IBytes(uint64(compressedBuf.Len())), humanize.IBytes(uint64(len(inputData))))

	compressedData := compressedBuf.Bytes()
	decompressedBuf := new(bytes.Buffer)

	bytesReader := bytes.NewReader(compressedData)
	sr := NewSaverReader(bytesReader)
	const bufsize = 8 * 1024
	buf := make([]byte, bufsize)

	var checkpoint *Checkpoint
	var readBytes int64
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
				break
			} else if err == ReadyToSaveError {
				var saveErr error
				checkpoint, saveErr = sr.Save()
				assert.NoError(t, saveErr)

				log.Printf("Made checkpoint at %s", humanize.IBytes(uint64(checkpoint.Roffset)))

				var resumeErr error
				bytesReader = bytes.NewReader(compressedData)
				sr, resumeErr = checkpoint.Resume(bytesReader)
				assert.NoError(t, resumeErr)
			} else {
				offset, _ := bytesReader.Seek(0, io.SeekCurrent)
				log.Printf("Got unrecoverable error at byte %d/%d", offset, len(compressedData))
				assert.NoError(t, err)
				t.FailNow()
			}
		}

		if readBytes > cts.threshold {
			sr.WantSave()
			readBytes = 0
		}
	}

	decompressedData := decompressedBuf.Bytes()
	assert.EqualValues(t, decompressedData, inputData)
}
