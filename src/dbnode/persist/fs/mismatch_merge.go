// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software withoutStream restriction, including withoutStream limitation the rights
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
	"bytes"
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
)

var errFinishedStreaming = errors.New("mismatch_streaming_finished")

type entry struct {
	idHash     uint64
	idChecksum uint32
	entry      schema.IndexEntry
}

func (e entry) toMismatch(t MismatchType) ReadMismatch {
	return ReadMismatch{
		Type:     t,
		Checksum: uint32(e.idChecksum),
		Data:     nil,                       // TODO: add these correctly.
		Tags:     nil,                       // TODO: add these correctly.
		ID:       ident.BytesID(e.entry.ID), // TODO: pool these correctly.
	}
}

type entryReader interface {
	next() bool
	current() entry
}

type streamMismatchWriter struct {
	iOpts instrument.Options
}

func newStreamMismatchWriter(iOpts instrument.Options) *streamMismatchWriter {
	return &streamMismatchWriter{iOpts: iOpts}
}

func (w *streamMismatchWriter) reportErrorDrainAndClose(
	err error,
	inStream <-chan ident.IndexHashBlock,
	outStream chan<- ReadMismatch,
) {
	outStream <- ReadMismatch{Type: MismatchError, Err: err}
	close(outStream)
	for range inStream {
		// no-op, drain input stream.
	}
}

func (w *streamMismatchWriter) drainRemainingBlockStreamAndClose(
	currentBatch []ident.IndexHash,
	inStream <-chan ident.IndexHashBlock,
	outStream chan<- ReadMismatch,
) {
	drain := func(hashes []ident.IndexHash) {
		for _, c := range hashes {
			outStream <- ReadMismatch{
				Type:     MismatchOnlyOnPrimary,
				Checksum: c.DataChecksum,
				IDHash:   c.IDHash,
			}
		}
	}

	drain(currentBatch)

	for batch := range inStream {
		drain(batch.IndexHashes)
	}

	close(outStream)
}

func (w *streamMismatchWriter) readRemainingReadersAndClose(
	current entry,
	reader entryReader,
	outStream chan<- ReadMismatch,
) {
	outStream <- current.toMismatch(MismatchOnlyOnSecondary)
	for reader.next() {
		outStream <- reader.current().toMismatch(MismatchOnlyOnSecondary)
	}

	close(outStream)
}

func (w *streamMismatchWriter) validate(c schema.IndexEntry, marker []byte, id, hID uint64) error {
	idMatch := bytes.Compare(c.ID, marker) == 0
	hashMatch := hID == id

	if idMatch && !hashMatch {
		// TODO: use a proper invariant here
		return fmt.Errorf(`invariant error: id "%s" hashed to both %d and %d`,
			string(c.ID), id, hID)
	}

	if !idMatch && hashMatch {
		return fmt.Errorf(`hash collision: "%s" and "%s" both hash to %d`,
			string(c.ID), string(marker), hID)
	}

	return nil
}

func (w *streamMismatchWriter) compareData(e entry, dataChecksum uint32, outStream chan<- ReadMismatch) {
	// NB: If data checksums match, this entry matches.
	if dataChecksum == uint32(e.entry.DataChecksum) {
		// here, dump contentious values.
		return
	}

	// Mark current entry as DATA_MISMATCH if there is a data mismatch.
	// here, add mismatches
	outStream <- e.toMismatch(MismatchData)
}

func (w *streamMismatchWriter) loadNextValidIndexHashBlock(
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) (ident.IndexHashBlock, error) {
	var (
		batch ident.IndexHashBlock
		ok    bool
	)

	curr := r.current()
	for {
		batch, ok = <-inStream
		if !ok {
			// NB: finished streaming from hash block. Mark remaining entries as
			// ONLY_SECONDARY and return.
			w.readRemainingReadersAndClose(curr, r, outStream)
			return ident.IndexHashBlock{}, errFinishedStreaming
		}

		if compare := bytes.Compare(batch.Marker, curr.entry.ID); compare > 0 {
			// NB: current element is before the current MARKER element;
			// this is a valid index hash block for comparison.
			return batch, nil
		} else if compare < 0 {
			// NB: all elements from the current idxHashBatch are before hte current
			// element; mark all elements in batch as ONLY_ON_PRIMARY and fetch the
			// next idxHashBatch.
			for _, c := range batch.IndexHashes {
				outStream <- ReadMismatch{
					Type:     MismatchOnlyOnPrimary,
					Checksum: c.DataChecksum,
					IDHash:   c.IDHash,
				}
			}

			continue
		}

		// NB: the last (i.e. MARKER) element is the first one in the index batch
		// to match the current element.
		lastIdx := len(batch.IndexHashes) - 1

		// NB: sanity check that matching IDs <=> matching ID hash.
		if err := w.validate(
			curr.entry, batch.Marker, curr.idHash,
			batch.IndexHashes[lastIdx].IDHash,
		); err != nil {
			return ident.IndexHashBlock{}, err
		}

		for i, idxHash := range batch.IndexHashes {
			// NB: Mark all preceeding entries as ONLY_PRIMARY mismatches.
			if lastIdx != i {
				outStream <- ReadMismatch{
					Type:     MismatchOnlyOnPrimary,
					Checksum: idxHash.DataChecksum,
					IDHash:   idxHash.IDHash,
				}

				continue
			}

			w.compareData(curr, idxHash.DataChecksum, outStream)
		}

		// Finished iterating through entry reader, drain any remaining entries
		// and return.
		if !r.next() {
			// NB: current batch already drained; pass empty here instead.
			w.drainRemainingBlockStreamAndClose([]ident.IndexHash{}, inStream, outStream)
			return ident.IndexHashBlock{}, errFinishedStreaming
		}

		curr = r.current()
	}
}

func (w *streamMismatchWriter) moveNext(
	currentBatch []ident.IndexHash,
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) error {
	if !r.next() {
		for _, c := range currentBatch {
			outStream <- ReadMismatch{
				Type:     MismatchOnlyOnPrimary,
				Checksum: c.DataChecksum,
				IDHash:   c.IDHash,
			}
		}

		// NB: no values in the entry reader
		batch, ok := <-inStream
		if ok {
			// NB: drain the input stream as fully ONLY_PRIMARY mismatches.
			w.drainRemainingBlockStreamAndClose(batch.IndexHashes, inStream, outStream)
		} else {
			// NB: no values in the input stream either, close the output stream.
			close(outStream)
		}

		return errFinishedStreaming
	}

	return nil
}

func (w *streamMismatchWriter) mergeHelper(
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) error {
	if err := w.moveNext([]ident.IndexHash{}, inStream, r, outStream); err != nil {
		return err
	}

	batch, err := w.loadNextValidIndexHashBlock(inStream, r, outStream)
	if err != nil {
		return err
	}

	batchIdx := 0
	markerIdx := len(batch.IndexHashes) - 1

	for {
		entry := r.current()
		hash := batch.IndexHashes[batchIdx]

		// NB: this is the last element in the batch. Check against MARKER.
		if batchIdx == markerIdx {
			if entry.idHash == hash.IDHash {
				// NB: sanity check that matching IDs <=> matching ID hash.
				if err := w.validate(
					entry.entry, batch.Marker, entry.idHash, hash.IDHash,
				); err != nil {
					return err
				}

				w.compareData(entry, hash.DataChecksum, outStream)

				// NB: get next reader element.
				// NB:  All entries from current batch already computed.
				if err := w.moveNext([]ident.IndexHash{}, inStream, r, outStream); err != nil {
					return err
				}

				// NB: get next index hash block, and reset batch and marker indices.
				batch, err = w.loadNextValidIndexHashBlock(inStream, r, outStream)
				if err != nil {
					return err
				}

				batchIdx = 0
				markerIdx = len(batch.IndexHashes) - 1
				continue
			}

			// NB: compare marker ID with current ID.
			compare := bytes.Compare(batch.Marker, entry.entry.ID)
			if compare == 0 {
				// NB: this is an error since hashed IDs mismatch here.
				return w.validate(entry.entry, batch.Marker, entry.idHash, hash.IDHash)
			} else if compare > 0 {
				// NB: current entry ID is before marker ID; mark the current entry
				// as a ONLY_SECONDARY mismatch and move to next reader element.
				outStream <- entry.toMismatch(MismatchOnlyOnSecondary)
				if err := w.moveNext(batch.IndexHashes[batchIdx:], inStream, r, outStream); err != nil {
					return err
				}

				continue
			}

			// NB: mark remaining in current batch as completed.
			for _, c := range batch.IndexHashes[batchIdx:] {
				outStream <- ReadMismatch{
					Type:     MismatchOnlyOnPrimary,
					Checksum: c.DataChecksum,
					IDHash:   c.IDHash,
				}
			}

			// NB: get next index hash block, and reset batch and marker indices.
			batch, err = w.loadNextValidIndexHashBlock(inStream, r, outStream)
			if err != nil {
				return err
			}

			batchIdx = 0
			markerIdx = len(batch.IndexHashes) - 1
		}

		if entry.idHash == hash.IDHash {
			batchIdx = batchIdx + 1
			w.compareData(entry, hash.DataChecksum, outStream)
			// NB: try to move next entry and next batch item.
			if !r.next() {
				remaining := batch.IndexHashes[batchIdx:]
				w.drainRemainingBlockStreamAndClose(remaining, inStream, outStream)
				return nil
			}

			// NB: move to next element.
			continue
		}

		nextBatchIdx := batchIdx + 1
		foundMatching := false
		for ; nextBatchIdx < markerIdx; nextBatchIdx++ {
			// NB: read next hashes, checking for index checksum matches.
			nextHash := batch.IndexHashes[nextBatchIdx]
			if entry.idHash == nextHash.IDHash {
				// NB: found matching checksum; add all indexHash entries between
				// batchIdx and nextBatchIdx as ONLY_PRIMARY mismatches.
				for _, c := range batch.IndexHashes[batchIdx:nextBatchIdx] {
					outStream <- ReadMismatch{
						Type:     MismatchOnlyOnPrimary,
						Checksum: c.DataChecksum,
						IDHash:   c.IDHash,
					}
				}

				batchIdx = nextBatchIdx + 1
				w.compareData(entry, nextHash.DataChecksum, outStream)
				// NB: try to move next entry and next batch item.
				if !r.next() {
					remaining := batch.IndexHashes[batchIdx:]
					w.drainRemainingBlockStreamAndClose(remaining, inStream, outStream)
					return nil
				}

				foundMatching = true
				break
			}
		}

		if foundMatching {
			continue
		}

		// NB: reached MATCHER point in the batch.
		nextHash := batch.IndexHashes[markerIdx]
		if entry.idHash == nextHash.IDHash {
			// NB: sanity check that matching IDs <=> matching ID hash.
			if err := w.validate(
				entry.entry, batch.Marker, entry.idHash, nextHash.IDHash,
			); err != nil {
				return err
			}

			// NB: Mark remaining entries in the batch as ONLY_PRIMARY mismatches.
			for _, c := range batch.IndexHashes[batchIdx:markerIdx] {
				outStream <- ReadMismatch{
					Type:     MismatchOnlyOnPrimary,
					Checksum: c.DataChecksum,
					IDHash:   c.IDHash,
				}
			}

			w.compareData(entry, nextHash.DataChecksum, outStream)

			// NB: get next reader element.
			// NB:  All entries from current batch already computed.
			if err := w.moveNext([]ident.IndexHash{}, inStream, r, outStream); err != nil {
				return err
			}

			// NB: get next index hash block, and reset batch and marker indices.
			batch, err = w.loadNextValidIndexHashBlock(inStream, r, outStream)
			if err != nil {
				return err
			}

			batchIdx = 0
			markerIdx = len(batch.IndexHashes) - 1
			continue
		}

		// NB: compare marker ID with current ID.
		compare := bytes.Compare(batch.Marker, entry.entry.ID)
		if compare == 0 {
			// NB: this is an error since hashed IDs mismatch here.
			return w.validate(entry.entry, batch.Marker, entry.idHash, nextHash.IDHash)
		}

		if compare > 0 {
			// NB: current entry ID is before marker ID; mark the current entry
			// as a ONLY_SECONDARY mismatch and move to next reader element.
			outStream <- entry.toMismatch(MismatchOnlyOnSecondary)
			// NB: get next reader element.
			if err := w.moveNext(batch.IndexHashes[markerIdx:], inStream, r, outStream); err != nil {
				return err
			}

			// NB: current entry ID is past marker ID; mark any remaining elements in
			// current batch as ONLY_PRIMARY, and increment batch.
			for _, c := range batch.IndexHashes[batchIdx:markerIdx] {
				fmt.Println("Mismatching here on", c.DataChecksum, "secondary", entry.entry.DataChecksum)
				// outStream <- ReadMismatch{
				// 	Type:     MismatchOnlyOnPrimary,
				// 	Checksum: c.DataChecksum,
				// 	IDHash:   c.IDHash,
				// }
			}

			// batchIdx++
			continue
		}

		// NB: get next index hash block, and reset batch and marker indices.
		batch, err = w.loadNextValidIndexHashBlock(inStream, r, outStream)
		if err != nil {
			return err
		}

		batchIdx = 0
		markerIdx = len(batch.IndexHashes) - 1
	}
}

func (w *streamMismatchWriter) merge(
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) error {
	err := w.mergeHelper(inStream, r, outStream)
	if err == nil {
		return nil
	}

	if err == errFinishedStreaming {
		return nil
	}

	w.reportErrorDrainAndClose(err, inStream, outStream)
	return err
}

/*























 */

func (w *streamMismatchWriter) drainBatchChecksums(
	checksums []uint32,
	outStream chan<- ReadMismatch,
) {
	for _, c := range checksums {
		outStream <- ReadMismatch{Checksum: c}
	}
}

func (w *streamMismatchWriter) drainRemainingBatchtreamAndCloseIdxChecksum(
	currentBatch []uint32,
	inStream <-chan ident.IndexChecksumBlock,
	outStream chan<- ReadMismatch,
) {
	w.drainBatchChecksums(currentBatch, outStream)

	for batch := range inStream {
		w.drainBatchChecksums(batch.Checksums, outStream)
	}

	close(outStream)
}

func (w *streamMismatchWriter) readRemainingReadersAndCloseIdxChecksum(
	current entry,
	reader entryReader,
	outStream chan<- ReadMismatch,
) {
	fmt.Println("writing remaining", current.toMismatch(MismatchOnlyOnSecondary))
	outStream <- current.toMismatch(MismatchOnlyOnSecondary)
	for reader.next() {
		outStream <- reader.current().toMismatch(MismatchOnlyOnSecondary)
	}

	close(outStream)
}

func (w *streamMismatchWriter) emitChecksumMismatches(
	e entry,
	checksum uint32,
	outStream chan<- ReadMismatch,
) {
	// NB: If data checksums match, this entry matches.
	if checksum == uint32(e.entry.DataChecksum) { // FIXME: index checksum here.
		return
	}

	outStream <- e.toMismatch(MismatchData)
}

func (w *streamMismatchWriter) loadNextValidIndexChecksumBatch(
	inStream <-chan ident.IndexChecksumBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) (ident.IndexChecksumBlock, bool) {
	var (
		batch ident.IndexChecksumBlock
		ok    bool
	)

	reader := r.current()
	for {
		batch, ok = <-inStream
		if !ok {
			// NB: finished streaming from hash block. Mark remaining entries as
			// ONLY_SECONDARY and return.
			w.readRemainingReadersAndCloseIdxChecksum(reader, r, outStream)
			return ident.IndexChecksumBlock{}, false
		}

		if len(batch.Checksums) == 0 {
			continue
		}

		fmt.Printf("BATCH %+v %b %s %s\n",
			batch, bytes.Compare(batch.Marker, reader.entry.ID), string(batch.Marker),
			string(reader.entry.ID))
		if compare := bytes.Compare(batch.Marker, reader.entry.ID); compare > 0 {
			// NB: current element is before the current MARKER element;
			// this is a valid index hash block for comparison.
			return batch, true
		} else if compare < 0 {
			// NB: all elements from the current idxHashBatch are before hte current
			// element; mark all elements in batch as ONLY_ON_PRIMARY and fetch the
			// next idxHashBatch.
			w.drainBatchChecksums(batch.Checksums, outStream)
			continue
		}

		// NB: the last (i.e. MARKER) element is the first one in the index batch
		// to match the current element.
		lastIdx := len(batch.Checksums) - 1
		if lastIdx >= 1 {
			// NB: Mark all preceeding entries as ONLY_PRIMARY mismatches.
			w.drainBatchChecksums(batch.Checksums[:lastIdx], outStream)
		}

		w.emitChecksumMismatches(reader, batch.Checksums[lastIdx], outStream)

		// NB: Increment entry read.
		if !w.nextReader(nil, inStream, r, outStream) {
			return ident.IndexChecksumBlock{}, false
		}

		reader = r.current()
	}
}

// nextReader increments the entry reader; if there are no additional entries,
// input stream and current batch are streamed to output as mismatches.
func (w *streamMismatchWriter) nextReader(
	currentBatch []uint32,
	inStream <-chan ident.IndexChecksumBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) bool {
	if r.next() {
		return true
	}

	// NB: if no next reader, drain remaining batches as mismatches
	// and close the input stream.
	w.drainRemainingBatchtreamAndCloseIdxChecksum(currentBatch, inStream, outStream)
	return false
}

func (w *streamMismatchWriter) mergeIdxChecksum(
	inStream <-chan ident.IndexChecksumBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) error {
	if !w.nextReader(nil, inStream, r, outStream) {
		// NB: no elements in the reader, channels drained.
		return nil
	}

	batch, hasBatch := w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
	if !hasBatch {
		// NB: no remaining batches in the input stream, channels drained.
		return nil
	}

	batchIdx := 0
	markerIdx := len(batch.Checksums) - 1
	for {
		entry := r.current()
		checksum := batch.Checksums[batchIdx]

		// NB: this is the last element in the batch. Check against MARKER.
		if batchIdx == markerIdx {
			if entry.idChecksum == checksum {
				if !bytes.Equal(batch.Marker, entry.entry.ID) {
					outStream <- entry.toMismatch(MismatchData)
				}

				// NB: advance to next reader element.
				if !w.nextReader(nil, inStream, r, outStream) {
					return nil
				}
			} else {
				compare := bytes.Compare(batch.Marker, entry.entry.ID)
				if compare == 0 {
					outStream <- entry.toMismatch(MismatchData)
				} else if compare > 0 {
					outStream <- entry.toMismatch(MismatchOnlyOnSecondary)
					// NB: advance to next reader element.
					if !w.nextReader(batch.Checksums[markerIdx:], inStream, r, outStream) {
						return nil
					}

					continue
				} else {
					outStream <- ReadMismatch{Checksum: checksum}
				}
			}

			batch, hasBatch = w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
			if !hasBatch {
				return nil
			}

			batchIdx = 0
			markerIdx = len(batch.Checksums) - 1
			continue
		}

		if entry.idChecksum == checksum {
			// NB: advance to next batch checksum.
			batchIdx++
			// NB: advance to next reader element.
			if !w.nextReader(batch.Checksums[batchIdx:], inStream, r, outStream) {
				return nil
			}

			continue
		}

		foundMatching := false
		for nextBatchIdx := batchIdx + 1; nextBatchIdx <= markerIdx; nextBatchIdx++ {
			// NB: read next hashes, checking for index checksum matches.
			nextChecksum := batch.Checksums[nextBatchIdx]
			fmt.Println(" entry.idChecksum != nextChecksum", entry.idChecksum, nextChecksum)
			fmt.Println(" nextBatchIdx, markerIdx", nextBatchIdx, markerIdx, batch)
			if entry.idChecksum != nextChecksum {
				continue
			}

			// NB: found matching checksum; add all indexHash entries between
			// batchIdx and nextBatchIdx as ONLY_PRIMARY mismatches.
			fmt.Println("draining", batch.Checksums[batchIdx:nextBatchIdx])
			w.drainBatchChecksums(batch.Checksums[batchIdx:nextBatchIdx], outStream)
			if nextBatchIdx != markerIdx {
				// NB: advance to next reader element.
				if !w.nextReader(nil, inStream, r, outStream) {
					return nil
				}

				batchIdx = nextBatchIdx + 1
			} else {
				if !bytes.Equal(batch.Marker, entry.entry.ID) {
					outStream <- entry.toMismatch(MismatchData)
				}

				// NB: advance to next reader element.
				if !w.nextReader(nil, inStream, r, outStream) {
					return nil
				}

				batch, hasBatch = w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
				if !hasBatch {
					return nil
				}

				batchIdx = 0
				markerIdx = len(batch.Checksums) - 1
			}

			foundMatching = true
			break
		}

		if foundMatching {
			continue
		}

		// NB: reached end of current batch with no match; use MARKER to determine
		// if the current element is missing from the batch checksums or if the
		// next batch should be loaded, instead.
		compare := bytes.Compare(batch.Marker, entry.entry.ID)
		if compare == 1 {
			// NB: this entry is missing from the batch; emit as a mismatch and
			// move to the next.
			fmt.Println("Writing mismatch", entry.toMismatch(MismatchOnlyOnSecondary))
			outStream <- entry.toMismatch(MismatchOnlyOnSecondary)
			// NB: advance to next reader element.
			if !w.nextReader(batch.Checksums[batchIdx:], inStream, r, outStream) {
				return nil
			}

			continue
		} else if compare < 0 {
			// NB: this entry is after the last entry in the batch. Drain remaining
			// batch elements as mismatches and increment the batch.
			w.drainBatchChecksums(batch.Checksums[batchIdx:], outStream)
		} else if compare == 0 {
			//  entry.idChecksum != batch.Checksums[markerIdx] {
			// NB: entry ID and marker IDs match, but checksums mismatch. Although
			// this means there is a mismatch here since matching IDs checksums
			// necessarily mismatch above, verify defensively here.
			fmt.Println("data mismatch", entry.toMismatch(MismatchOnlyOnSecondary), batch.Checksums[batchIdx:markerIdx])
			w.drainBatchChecksums(batch.Checksums[batchIdx:markerIdx], outStream)
			if entry.idChecksum != batch.Checksums[markerIdx] {
				outStream <- entry.toMismatch(MismatchData)
			}

			if !w.nextReader(batch.Checksums[batchIdx:markerIdx-1], inStream, r, outStream) {
				return nil
			}
		} else {
			if entry.idChecksum != batch.Checksums[markerIdx] {
				outStream <- entry.toMismatch(MismatchData)
			}
		}

		// NB: get next index hash block, and reset batch and marker indices.
		batch, hasBatch = w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
		if !hasBatch {
			return nil
		}

		batchIdx = 0
		markerIdx = len(batch.Checksums) - 1
	}
}
