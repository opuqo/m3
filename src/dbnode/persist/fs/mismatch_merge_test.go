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
	"fmt"
	"sync"
	"testing"

	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"

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
	t *testing.T, expected []ReadMismatch,
) (chan<- ReadMismatch, *sync.WaitGroup) {
	ch := make(chan ReadMismatch)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		actual := make([]ReadMismatch, 0, len(expected))
		for mismatch := range ch {
			actual = append(actual, mismatch)
			fmt.Printf("%+v\n", mismatch)
		}
		assert.Equal(t, expected, actual,
			fmt.Sprintf("mismatch lists do not match\n\nExpected: %+v\nActual:   %+v",
				expected, actual))
		wg.Done()
	}()

	return ch, &wg
}

func idxEntry(i int, id string) entry {
	return entry{
		entry: schema.IndexEntry{
			ID: []byte(id),
		},
		idChecksum: uint32(i),
	}
}

func mismatch(t MismatchType, i int, id []byte) ReadMismatch {
	m := ReadMismatch{Checksum: uint32(i)}
	if len(id) > 0 {
		m.ID = ident.BytesID(id)
	}

	return m
}

func buildDataInputStream(
	bls []ident.IndexChecksumBlock,
) <-chan ident.IndexChecksumBlock {
	ch := make(chan ident.IndexChecksumBlock)
	go func() {
		for _, bl := range bls {
			ch <- bl
		}
		close(ch)
	}()

	return ch
}

func assertClosed(t *testing.T, in <-chan ident.IndexChecksumBlock) {
	_, isOpen := <-in
	require.False(t, isOpen)
}

func TestDrainRemainingBlockStreamAndClose(t *testing.T) {
	batch := []uint32{1, 2}
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		{Checksums: []uint32{3, 4}},
		{Checksums: []uint32{5}},
		{Checksums: []uint32{}},
		{Checksums: []uint32{6}},
	})
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
		{Checksum: 3},
		{Checksum: 4},
		{Checksum: 5},
		{Checksum: 6},
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	w.drainRemainingBatchtreamAndClose(batch, inStream, outStream)
	assertClosed(t, inStream)
}

func TestReadRemainingReadersAndClose(t *testing.T) {
	entry := idxEntry(1, "bar")
	reader := newEntryReaders(idxEntry(2, "foo"), idxEntry(3, "qux"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(MismatchOnlyOnSecondary, 1, []byte("bar")),
		mismatch(MismatchOnlyOnSecondary, 2, []byte("foo")),
		mismatch(MismatchOnlyOnSecondary, 3, []byte("qux")),
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	w.readRemainingReadersAndClose(entry, reader, outStream)
}

func TestEmitChecksumMismatches(t *testing.T) {
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(MismatchData, 2, []byte("def")),
	})

	w := newStreamMismatchWriter(instrument.NewOptions())
	w.emitChecksumMismatches(idxEntry(1, "bar"), 1, outStream)
	w.emitChecksumMismatches(idxEntry(2, "def"), 1, outStream)

	// NB: outStream not closed naturally in this subfunction; close explicitly.
	close(outStream)
	wg.Wait()
}

func TestLoadNextValidIndexChecksumBatch(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		{Checksums: []uint32{}, Marker: []byte("aaa")},
		{Checksums: []uint32{3, 4}, Marker: []byte("foo")},
	})

	reader := newEntryReaders(idxEntry(1, "abc"), idxEntry(2, "def"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(MismatchOnlyOnSecondary, 1, []byte("abc")),
		mismatch(MismatchOnlyOnSecondary, 2, []byte("def")),
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	assert.True(t, reader.next())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.Equal(t, []uint32{3, 4}, bl.Checksums)
	assert.Equal(t, "foo", string(bl.Marker))
	bl, hasNext = w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(bl.Marker))
	assertClosed(t, inStream)
}

func TestLoadNextWithExhaustedInput(t *testing.T) {
	inStream := make(chan ident.IndexChecksumBlock)
	close(inStream)

	reader := newEntryReaders(idxEntry(1, "abc"), idxEntry(2, "def"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(MismatchOnlyOnSecondary, 1, []byte("abc")),
		mismatch(MismatchOnlyOnSecondary, 2, []byte("def")),
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	assert.True(t, reader.next())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(bl.Marker))
}

func TestLoadNextValidIndexHashBlockValid(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		{Checksums: []uint32{1, 2}, Marker: []byte("zztop")},
	})
	reader := newEntryReaders(idxEntry(10, "abc"))
	require.True(t, reader.next())

	// NB: outStream not used in this path; close explicitly.
	outStream, _ := buildExpectedOutputStream(t, []ReadMismatch{})
	close(outStream)

	w := newStreamMismatchWriter(instrument.NewOptions())

	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.True(t, hasNext)
	assert.Equal(t, []uint32{1, 2}, bl.Checksums)
	assert.Equal(t, "zztop", string(bl.Marker))
}

func TestLoadNextValidIndexHashBlockSkipThenValid(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("aardvark"),
		Checksums: []uint32{1, 2},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("zztop"),
		Checksums: []uint32{3, 4},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2})
	reader := newEntryReaders(idxEntry(10, "abc"))
	require.True(t, reader.next())

	// NB: entire first block should be ONLY_ON_PRIMARY.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.True(t, hasNext)
	assert.Equal(t, []uint32{3, 4}, bl.Checksums)
	assert.Equal(t, "zztop", string(bl.Marker))

	// NB: outStream not closed in this path; close explicitly.
	close(outStream)
}

func TestLoadNextValidIndexHashBlockSkipsExhaustive(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("aardvark"),
		Checksums: []uint32{1, 2},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("abc"),
		Checksums: []uint32{3, 4},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2})
	reader := newEntryReaders(idxEntry(10, "zztop"), idxEntry(0, "zzz"))
	require.True(t, reader.next())

	// NB: entire first block should be ONLY_ON_PRIMARY,
	// entire secondary block should be ONLY_ON_SECONDARY.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
		{Checksum: 3},
		{Checksum: 4},
		mismatch(MismatchOnlyOnSecondary, 10, []byte("zztop")),
		mismatch(MismatchOnlyOnSecondary, 0, []byte("zzz")),
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(string(bl.Marker)))
}

func TestLoadNextValidIndexHashBlockLastElementMatch(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{{
		Marker:    []byte("abc"),
		Checksums: []uint32{1, 2, 3},
	}})
	reader := newEntryReaders(idxEntry(3, "abc"))
	require.True(t, reader.next())

	// NB: values preceeding MARKER in index hash are only on primary.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(string(bl.Marker)))
}

func TestLoadNextValidIndexHashBlock(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("a"),
		Checksums: []uint32{1, 2, 3},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("b"),
		Checksums: []uint32{4, 5},
	}

	bl3 := ident.IndexChecksumBlock{
		Marker:    []byte("d"),
		Checksums: []uint32{6, 7},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2, bl3})
	reader := newEntryReaders(idxEntry(5, "b"), idxEntry(10, "c"))
	require.True(t, reader.next())

	// Values preceeding MARKER in second index hash block are only on primary.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
		{Checksum: 3},
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.True(t, hasNext)
	assert.Equal(t, bl3, bl)

	// NB: outStream not closed in this path; close explicitly.
	close(outStream)
}

func TestMergeHelper(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("a"),
		Checksums: []uint32{2, 3},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("d"),
		Checksums: []uint32{12, 4, 5},
	}

	bl3 := ident.IndexChecksumBlock{
		Marker:    []byte("mismatch"),
		Checksums: []uint32{6},
	}

	bl4 := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []uint32{1},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		bl1, bl2, bl3, bl4})
	mismatched := entry{
		entry: schema.IndexEntry{
			ID: []byte("mismatch"),
		},
		idChecksum: 88,
	}

	reader := newEntryReaders(
		idxEntry(12, "b"),
		idxEntry(5, "d"),
		mismatched,
		idxEntry(7, "qux"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		// Values preceeding MARKER in second index hash block are only on primary.
		{Checksum: 2},
		{Checksum: 3},
		{Checksum: 4},
		// Value at 5 matches, not in output.
		// Value at 6 is a DATA_MISMATCH
		ReadMismatch{Checksum: 88, ID: ident.StringID("mismatch")},
		// Value at 10 only on secondary.
		mismatch(MismatchOnlyOnSecondary, 7, []byte("qux")),
		// Value at 7 only on primary.
		{Checksum: 1},
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeIntermittentBlock(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{{
		Marker:    []byte("z"),
		Checksums: []uint32{1, 2, 3, 4},
	}})
	reader := newEntryReaders(
		idxEntry(2, "n"),
		idxEntry(4, "y"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 3},
		mismatch(MismatchData, 4, []byte("y")),
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeIntermittentBlockMismatch(t *testing.T) {
	bl := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []uint32{1, 2, 3, 4},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl})
	reader := newEntryReaders(
		idxEntry(2, "n"),
		idxEntry(5, "z"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(MismatchOnlyOnSecondary, 2, []byte("n")),
		mismatch(MismatchData, 5, []byte("z")),
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeTrailingBlock(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("m"),
		Checksums: []uint32{1, 2},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []uint32{3, 5},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2})
	reader := newEntryReaders(
		idxEntry(1, "a"),
		idxEntry(4, "w"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 2},
		mismatch(MismatchOnlyOnSecondary, 4, []byte("w")),
		{Checksum: 3},
		{Checksum: 5},
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMerge(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("c"),
		Checksums: []uint32{1, 2, 3},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("f"),
		Checksums: []uint32{4, 5},
	}

	bl3 := ident.IndexChecksumBlock{
		Marker:    []byte("p"),
		Checksums: []uint32{6, 7, 8, 9},
	}

	bl4 := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []uint32{11, 15},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2, bl3, bl4})
	missEntry := idxEntry(3, "c")
	missEntry.idChecksum = 100
	reader := newEntryReaders(
		idxEntry(1, "a"),
		missEntry,
		idxEntry(7, "n"),
		idxEntry(8, "p"),
		idxEntry(9, "p"),
		idxEntry(10, "q"),
		idxEntry(15, "z"),
	)
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 2},
		ReadMismatch{Checksum: 100, ID: ident.StringID("c")},
		{Checksum: 4},
		{Checksum: 5},
		{Checksum: 6},
		mismatch(MismatchOnlyOnSecondary, 10, []byte("q")),
		{Checksum: 11},
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeIntermittentBlockJagged(t *testing.T) {
	bl := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []uint32{1, 2, 4, 6},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl})
	reader := newEntryReaders(
		idxEntry(1, "b"),
		idxEntry(3, "f"),
		idxEntry(5, "w"),
		idxEntry(6, "z"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnSecondary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
		mismatch(MismatchOnlyOnSecondary, 5, nil),
	})
	defer wg.Wait()

	w := newStreamMismatchWriter(instrument.NewOptions())
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}
