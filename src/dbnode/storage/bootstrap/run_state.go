package bootstrap

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
)

var (
	errFilesystemOptsNotSet   = errors.New("filesystemOptions not set")
	errInfoFilesFindersNotSet = errors.New("infoFilesFinders not set")
)

// NewRunState creates state specifically to be used during the bootstrap process.
// Primarily a mechanism for passing info files along without needing to re-read them at each
// stage of the bootstrap process.
func NewRunState(options RunStateOptions) (RunState, error) {
	// TODO(nate): enable this once properly instantiating in NamespacesTester in util.go
	// if err := options.Validate(); err != nil {
	//   return RunState{}, err
	// }
	return RunState{
		fsOpts:           options.FilesystemOptions(),
		infoFilesFinders: options.InfoFilesFinders(),
	}, nil
}

// InfoFilesForNamespace returns the info files grouped by shard for the provided namespace.
func (r *RunState) InfoFilesForNamespace(ns namespace.Metadata) InfoFileResultsPerShard {
	return r.ReadInfoFiles()[ns]
}

// TODO(nate): Make this threadsafe? If so, we'll need to clone the map
// before returning, provide an update method, and incorporate locking.
//
// ReadInfoFiles returns info file results for each shard grouped by namespace. A cached copy
// is returned if the info files have already been read. A re-fetch can be triggered by passing
// in invalidateCache as true.
func (r *RunState) ReadInfoFiles() InfoFilesByNamespace {
	if r.infoFilesByNamespace != nil {
		return r.infoFilesByNamespace
	}

	r.infoFilesByNamespace = make(InfoFilesByNamespace, len(r.infoFilesFinders))
	for _, finder := range r.infoFilesFinders {
		result := make(InfoFileResultsPerShard, len(finder.Shards))
		for _, shard := range finder.Shards {
			result[shard] = fs.ReadInfoFiles(r.fsOpts.FilePathPrefix(),
				finder.Namespace.ID(), shard, r.fsOpts.InfoReaderBufferSize(), r.fsOpts.DecodingOptions(),
				persist.FileSetFlushType)
		}

		r.infoFilesByNamespace[finder.Namespace] = result
	}

	return r.infoFilesByNamespace
}

type runStateOptions struct {
	fsOpts           fs.Options
	infoFilesFinders []InfoFilesFinder
}

// NewRunStateOptions creates new RunStateOptions.
func NewRunStateOptions() RunStateOptions {
	return &runStateOptions{}
}

func (r *runStateOptions) Validate() error {
	if r.fsOpts == nil {
		return errFilesystemOptsNotSet
	}
	if err := r.fsOpts.Validate(); err != nil {
		return err
	}
	if len(r.infoFilesFinders) == 0 {
		return errInfoFilesFindersNotSet
	}
	return nil
}

func (r *runStateOptions) SetFilesystemOptions(value fs.Options) RunStateOptions {
	opts := *r
	opts.fsOpts = value
	return &opts
}

func (r *runStateOptions) FilesystemOptions() fs.Options {
	return r.fsOpts
}

func (r *runStateOptions) SetInfoFilesFinders(value []InfoFilesFinder) RunStateOptions {
	opts := *r
	opts.infoFilesFinders = value
	return &opts
}

func (r *runStateOptions) InfoFilesFinders() []InfoFilesFinder {
	return r.infoFilesFinders
}
