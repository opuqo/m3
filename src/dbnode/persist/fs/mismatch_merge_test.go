// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package fs

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type entryIter struct {
	idx     int
	entries []entry
}

func (it *entryIter) next() bool {
	it.idx = it.idx + 1
	return it.idx < len(it.entries)
}

func (it *entryIter) current() entry { return it.entries[it.idx] }

func (it *entryIter) assertExhausted(t *testing.T) {
	assert.True(t, it.idx >= len(it.entries))
}

func newEntryReaders(entries ...entry) entryReader {
	return &entryIter{idx: -1, entries: entries}
}

func buildExpectedOutputStream(
	t *testing.T, expected ReadMismatches,
) (chan<- ReadMismatch, *sync.WaitGroup) {
	ch := make(chan ReadMismatch)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		actual := make(ReadMismatches, 0, len(expected))
		for mismatch := range ch {
			actual = append(actual, mismatch)
		}
		assert.Equal(t, expected, actual,
			fmt.Sprintf("mismatch lists do not match\n\nExpected: %+v\nActual:   %+v",
				expected, actual))
		wg.Done()
	}()

	return ch, &wg
}

func buildDataInputStream(bls []ident.IndexHashBlock) <-chan ident.IndexHashBlock {
	ch := make(chan ident.IndexHashBlock)
	go func() {
		for _, bl := range bls {
			ch <- bl
		}
		close(ch)
	}()

	return ch
}

func idxHash(i int) ident.IndexHash {
	return ident.IndexHash{DataChecksum: uint32(i), IDHash: uint64(i)}
}

func idxEntry(i int, id string) entry {
	return entry{
		entry: schema.IndexEntry{
			DataChecksum: int64(i),
			ID:           []byte(id),
		},
		idHash: uint64(i),
	}
}

func mismatch(t MismatchType, i int, id []byte) ReadMismatch {
	m := ReadMismatch{Type: t, Checksum: uint32(i), IDHash: uint64(i)}
	if len(id) > 0 {
		m.ID = ident.BytesID(id)
	}

	return m
}

func assertClosed(t *testing.T, in <-chan ident.IndexHashBlock) {
	_, isOpen := <-in
	require.False(t, isOpen)
}

func TestReportErrorDrainAndClose(t *testing.T) {
	err := errors.New("an error")
	inStream := buildDataInputStream([]ident.IndexHashBlock{
		{IndexHashes: []ident.IndexHash{idxHash(3), idxHash(4)}},
		{IndexHashes: []ident.IndexHash{idxHash(5)}},
		{IndexHashes: []ident.IndexHash{}},
		{IndexHashes: []ident.IndexHash{idxHash(6)}},
	})
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		ReadMismatch{Type: MismatchError, Err: err},
	})
	defer wg.Wait()

	// NB: this will verify that inStream is drained.
	reportErrorDrainAndClose(err, inStream, outStream)
	assertClosed(t, inStream)
}

func TestDrainRemainingBlockStreamAndClose(t *testing.T) {
	batch := []ident.IndexHash{idxHash(1), idxHash(2)}
	inStream := buildDataInputStream([]ident.IndexHashBlock{
		{IndexHashes: []ident.IndexHash{idxHash(3), idxHash(4)}},
		{IndexHashes: []ident.IndexHash{idxHash(5)}},
		{IndexHashes: []ident.IndexHash{}},
		{IndexHashes: []ident.IndexHash{idxHash(6)}},
	})
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
		mismatch(MismatchOnlyOnPrimary, 5, nil),
		mismatch(MismatchOnlyOnPrimary, 6, nil),
	})
	defer wg.Wait()

	drainRemainingBlockStreamAndClose(batch, inStream, outStream)
	assertClosed(t, inStream)
}

func TestReadRemainingReadersAndClose(t *testing.T) {
	entry := idxEntry(1, "bar")
	reader := newEntryReaders(idxEntry(2, "foo"), idxEntry(3, "qux"))
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnSecondary, 1, []byte("bar")),
		mismatch(MismatchOnlyOnSecondary, 2, []byte("foo")),
		mismatch(MismatchOnlyOnSecondary, 3, []byte("qux")),
	})
	defer wg.Wait()

	readRemainingReadersAndClose(entry, reader, outStream)
}

func TestValidate(t *testing.T) {
	foo, bar := []byte("foo"), []byte("bar")
	err := validate(schema.IndexEntry{ID: foo}, foo, 9, 9)
	assert.NoError(t, err)

	err = validate(schema.IndexEntry{ID: foo}, foo, 10, 9)
	assert.Error(t, err)

	err = validate(schema.IndexEntry{ID: foo}, bar, 9, 9)
	assert.Error(t, err)
}

func TestCompareData(t *testing.T) {
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchData, 2, []byte("def")),
	})

	// checksum match
	compareData(idxEntry(1, "abc"), 1, outStream)
	// checksum mismatch
	compareData(idxEntry(2, "def"), 3, outStream)

	// NB: outStream not closed naturally in this subfunction; close explicitly.
	close(outStream)
	wg.Wait()
}

func TestMoveNextWithRemainingInput(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexHashBlock{
		{IndexHashes: []ident.IndexHash{idxHash(3), idxHash(4)}},
	})
	reader := newEntryReaders(idxEntry(1, "abc"))
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
	})
	defer wg.Wait()

	curr := []ident.IndexHash{idxHash(2)}
	assert.NoError(t, moveNext(curr, inStream, reader, outStream))
	assert.Equal(t, errFinishedStreaming, moveNext(curr, inStream, reader, outStream))
	assertClosed(t, inStream)
}

func TestMoveNextWithExhaustedInput(t *testing.T) {
	inStream := make(chan ident.IndexHashBlock)
	close(inStream)

	reader := newEntryReaders(idxEntry(1, "abc"))
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 2, nil),
	})
	defer wg.Wait()

	curr := []ident.IndexHash{idxHash(2)}
	assert.NoError(t, moveNext(curr, inStream, reader, outStream))
	assert.Equal(t, errFinishedStreaming, moveNext(curr, inStream, reader, outStream))
}

func TestLoadNextValidIndexHashBlockExhaustedInput(t *testing.T) {
	inStream := make(chan ident.IndexHashBlock)
	close(inStream)

	reader := newEntryReaders(idxEntry(1, "abc"), idxEntry(2, "def"))
	require.True(t, reader.next())

	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnSecondary, 1, []byte("abc")),
		mismatch(MismatchOnlyOnSecondary, 2, []byte("def")),
	})
	defer wg.Wait()

	_, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.Equal(t, errFinishedStreaming, err)
}

func TestLoadNextValidIndexHashBlockValid(t *testing.T) {
	bl := ident.IndexHashBlock{
		Marker:      []byte("zztop"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl})
	reader := newEntryReaders(idxEntry(10, "abc"))
	require.True(t, reader.next())

	// NB: outStream not used in this path; close explicitly.
	outStream, _ := buildExpectedOutputStream(t, ReadMismatches{})
	close(outStream)

	nextBlock, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.NoError(t, err)
	assert.Equal(t, bl, nextBlock)

}

func TestLoadNextValidIndexHashBlockSkipThenValid(t *testing.T) {
	bl1 := ident.IndexHashBlock{
		Marker:      []byte("aardvark"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2)},
	}

	bl2 := ident.IndexHashBlock{
		Marker:      []byte("zztop"),
		IndexHashes: []ident.IndexHash{idxHash(3), idxHash(4)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl1, bl2})
	reader := newEntryReaders(idxEntry(10, "abc"))
	require.True(t, reader.next())

	// NB: entire first block should be ONLY_ON_PRIMARY.
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
	})
	defer wg.Wait()

	nextBlock, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.NoError(t, err)
	assert.Equal(t, bl2, nextBlock)

	// NB: outStream not closed in this path; close explicitly.
	close(outStream)
}

func TestLoadNextValidIndexHashBlockSkipsExhaustive(t *testing.T) {
	bl1 := ident.IndexHashBlock{
		Marker:      []byte("aardvark"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2)},
	}

	bl2 := ident.IndexHashBlock{
		Marker:      []byte("abc"),
		IndexHashes: []ident.IndexHash{idxHash(3), idxHash(4)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl1, bl2})
	reader := newEntryReaders(idxEntry(10, "zztop"), idxEntry(0, "zzz"))
	require.True(t, reader.next())

	// NB: entire first block should be ONLY_ON_PRIMARY,
	// entire secondary block should be ONLY_ON_SECONDARY.
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
		mismatch(MismatchOnlyOnSecondary, 10, []byte("zztop")),
		mismatch(MismatchOnlyOnSecondary, 0, []byte("zzz")),
	})
	defer wg.Wait()

	_, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.Equal(t, errFinishedStreaming, err)
}

func TestLoadNextValidIndexHashBlockLastElementInvariant(t *testing.T) {
	bl := ident.IndexHashBlock{
		Marker:      []byte("abc"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl})
	reader := newEntryReaders(idxEntry(10, "abc"))
	require.True(t, reader.next())

	// NB: outStream not used in this path; close explicitly.
	outStream, _ := buildExpectedOutputStream(t, ReadMismatches{})
	close(outStream)

	_, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "invariant"))
}

func TestLoadNextValidIndexHashBlockLastElementMatch(t *testing.T) {
	bl := ident.IndexHashBlock{
		Marker:      []byte("abc"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2), idxHash(3)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl})
	reader := newEntryReaders(idxEntry(3, "abc"))
	require.True(t, reader.next())

	// NB: values preceeding MARKER in index hash are only on primary.
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
	})
	defer wg.Wait()

	_, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.Equal(t, errFinishedStreaming, err)
}

func TestLoadNextValidIndexHashBlock(t *testing.T) {
	bl1 := ident.IndexHashBlock{
		Marker:      []byte("a"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2), idxHash(3)},
	}

	bl2 := ident.IndexHashBlock{
		Marker:      []byte("b"),
		IndexHashes: []ident.IndexHash{idxHash(4), idxHash(5)},
	}

	bl3 := ident.IndexHashBlock{
		Marker:      []byte("d"),
		IndexHashes: []ident.IndexHash{idxHash(6), idxHash(7)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl1, bl2, bl3})
	reader := newEntryReaders(idxEntry(5, "b"), idxEntry(10, "c"))
	require.True(t, reader.next())

	// Values preceeding MARKER in second index hash block are only on primary.
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
	})
	defer wg.Wait()

	bl, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.NoError(t, err)
	assert.Equal(t, bl3, bl)

	// NB: outStream not closed in this path; close explicitly.
	close(outStream)
}

func TestMergeHelper(t *testing.T) {
	bl1 := ident.IndexHashBlock{
		Marker:      []byte("a"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2), idxHash(3)},
	}

	bl2 := ident.IndexHashBlock{
		Marker:      []byte("b"),
		IndexHashes: []ident.IndexHash{idxHash(4), idxHash(5)},
	}

	bl3 := ident.IndexHashBlock{
		Marker:      []byte("z"),
		IndexHashes: []ident.IndexHash{idxHash(6), idxHash(10)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl1, bl2, bl3})
	mismatched := entry{
		entry: schema.IndexEntry{
			DataChecksum: 88,
			ID:           []byte("mismatch"),
		},
		idHash: 6,
	}

	reader := newEntryReaders(idxEntry(5, "b"), mismatched, idxEntry(7, "qux"))
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		// Values preceeding MARKER in second index hash block are only on primary.
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
		// Value at 5 matches, not in output.
		// Value at 6 is a DATA_MISMATCH
		ReadMismatch{Type: MismatchData, Checksum: 88, IDHash: 6, ID: ident.StringID("mismatch")},
		// Value at 10 only on secondary.
		mismatch(MismatchOnlyOnSecondary, 7, []byte("qux")),
		// Value at 7 only on primary.
		mismatch(MismatchOnlyOnPrimary, 10, nil),
	})
	defer wg.Wait()

	err := mergeHelper(inStream, reader, outStream)
	assert.Equal(t, errFinishedStreaming, err)
}

func TestMergeIntermittentBlock(t *testing.T) {
	bl := ident.IndexHashBlock{
		Marker:      []byte("z"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2), idxHash(3), idxHash(4)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl})
	reader := newEntryReaders(
		idxEntry(2, "n"),
		idxEntry(4, "z"),
	)

	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
	})
	defer wg.Wait()

	err := merge(inStream, reader, outStream)
	assert.NoError(t, err)
}

func TestMergeIntermittentBlockMismatch(t *testing.T) {
	bl := ident.IndexHashBlock{
		Marker:      []byte("z"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2), idxHash(3), idxHash(4)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl})
	reader := newEntryReaders(
		idxEntry(2, "n"),
		idxEntry(5, "z"),
	)

	expectedErr := errors.New(`invariant error: id "z" hashed to both 5 and 4`)
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		ReadMismatch{Type: MismatchError, Err: expectedErr},
	})
	defer wg.Wait()

	err := merge(inStream, reader, outStream)
	assert.Equal(t, expectedErr, err)
}

func TestMergeTrailingBlock(t *testing.T) {
	bl1 := ident.IndexHashBlock{
		Marker:      []byte("m"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2)},
	}

	bl2 := ident.IndexHashBlock{
		Marker:      []byte("z"),
		IndexHashes: []ident.IndexHash{idxHash(3), idxHash(5)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl1, bl2})
	reader := newEntryReaders(
		idxEntry(1, "a"),
		idxEntry(4, "w"),
	)

	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
		mismatch(MismatchOnlyOnSecondary, 4, []byte("w")),
		mismatch(MismatchOnlyOnPrimary, 5, nil),
	})
	defer wg.Wait()

	err := merge(inStream, reader, outStream)
	assert.NoError(t, err)
}

func TestMerge(t *testing.T) {
	bl1 := ident.IndexHashBlock{
		Marker:      []byte("c"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2), idxHash(3)},
	}

	bl2 := ident.IndexHashBlock{
		Marker:      []byte("f"),
		IndexHashes: []ident.IndexHash{idxHash(4), idxHash(5)},
	}

	bl3 := ident.IndexHashBlock{
		Marker:      []byte("p"),
		IndexHashes: []ident.IndexHash{idxHash(6), idxHash(7), idxHash(8), idxHash(9)},
	}

	bl4 := ident.IndexHashBlock{
		Marker:      []byte("z"),
		IndexHashes: []ident.IndexHash{idxHash(11), idxHash(15)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl1, bl2, bl3, bl4})
	missEntry := idxEntry(3, "c")
	missEntry.entry.DataChecksum = 100
	reader := newEntryReaders(
		idxEntry(1, "a"),
		missEntry,
		idxEntry(7, "n"),
		idxEntry(8, "p"),
		idxEntry(9, "p"),
		idxEntry(10, "q"),
		idxEntry(15, "z"),
	)
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		ReadMismatch{Type: MismatchData, Checksum: 100, IDHash: 3, ID: ident.StringID("c")},
		mismatch(MismatchOnlyOnPrimary, 4, nil),
		mismatch(MismatchOnlyOnPrimary, 5, nil),
		mismatch(MismatchOnlyOnPrimary, 6, nil),
		mismatch(MismatchOnlyOnPrimary, 11, nil),
		mismatch(MismatchOnlyOnSecondary, 10, []byte("q")),
	})
	defer wg.Wait()

	err := merge(inStream, reader, outStream)
	assert.NoError(t, err)
}

func TestMergeIntermittentBlockJagged(t *testing.T) {
	bl := ident.IndexHashBlock{
		Marker:      []byte("z"),
		IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2), idxHash(4), idxHash(6)},
	}

	inStream := buildDataInputStream([]ident.IndexHashBlock{bl})
	reader := newEntryReaders(
		idxEntry(1, "b"),
		idxEntry(3, "f"),
		idxEntry(5, "w"),
		idxEntry(6, "z"),
	)

	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnSecondary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
		mismatch(MismatchOnlyOnSecondary, 5, nil),
	})
	defer wg.Wait()

	err := merge(inStream, reader, outStream)
	assert.NoError(t, err)
}
