package crawler

import (
	"context"
	"crawler/internal/fs"
	"crawler/internal/workerpool"
	"encoding/json"
	"fmt"
	"sync"
)

// Configuration holds the configuration for the crawler, specifying the number of workers for
// file searching, processing, and accumulating tasks. The values for SearchWorkers, FileWorkers,
// and AccumulatorWorkers are critical to efficient performance and must be defined in
// every configuration.
type Configuration struct {
	SearchWorkers      int // Number of workers responsible for searching files.
	FileWorkers        int // Number of workers for processing individual files.
	AccumulatorWorkers int // Number of workers for accumulating results.
}

// Combiner is a function type that defines how to combine two values of type R into a single
// result. Combiner is not required to be thread-safe
//
// Combiner can either:
//   - Modify one of its input arguments to include the result of the other and return it,
//     or
//   - Create a new combined result based on the inputs and return it.
//
// It is assumed that type R has a neutral element (forming a monoid)
type Combiner[R any] func(current R, accum R) R

// Crawler represents a concurrent crawler implementing a map-reduce model with multiple workers
// to manage file processing, transformation, and accumulation tasks. The crawler is designed to
// handle large sets of files efficiently, assuming that all files can fit into memory
// simultaneously.
type Crawler[T, R any] interface {
	// Collect performs the full crawling operation, coordinating with the file system
	// and worker pool to process files and accumulate results. The result type R is assumed
	// to be a monoid, meaning there exists a neutral element for combination, and that
	// R supports an associative combiner operation.
	// The result of this collection process, after all reductions, is returned as type R.
	//
	// Important requirements:
	// 1. Number of workers in the Configuration is mandatory for managing workload efficiently.
	// 2. FileSystem and Accumulator must be thread-safe.
	// 3. Combiner does not need to be thread-safe.
	// 4. If an accumulator or combiner function modifies one of its arguments,
	//    it should return that modified value rather than creating a new one,
	//    or alternatively, it can create and return a new combined result.
	// 5. Context cancellation is respected across workers.
	// 6. Type T is derived by json-deserializing the file contents, and any issues in deserialization
	//    must be handled within the worker.
	// 7. The combiner function will wait for all workers to complete, ensuring no goroutine leaks
	//    occur during the process.
	Collect(
		ctx context.Context,
		fileSystem fs.FileSystem,
		root string,
		conf Configuration,
		accumulator workerpool.Accumulator[T, R],
		combiner Combiner[R],
	) (R, error)
}

type crawlerImpl[T, R any] struct{}

func New[T, R any]() *crawlerImpl[T, R] {
	return &crawlerImpl[T, R]{}
}

type errContext struct {
	err   error
	errCh chan error
}
type file struct {
	name  string
	isDir bool
}

func catchPanic(once *sync.Once, errCtx *errContext) {
	if r := recover(); r != nil {
		var err error
		if v, ok := r.(error); ok {
			err = v
		} else {
			err = fmt.Errorf("%v", r)
		}
		once.Do(func() { writeErr(err, errCtx) })
	}
}

func searcher(fileSystem fs.FileSystem, files chan<- file, errCtx *errContext, once *sync.Once) func(parent file) []file {
	return func(parent file) []file {
		var children []file
		defer catchPanic(once, errCtx)
		if parent.isDir {
			ch, err := fileSystem.ReadDir(parent.name)
			if err != nil {
				once.Do(func() { writeErr(err, errCtx) })
			}
			for _, chs := range ch {
				children = append(children, file{fileSystem.Join(parent.name, chs.Name()), chs.IsDir()})
			}
		} else {
			select {
			case files <- parent:
			case <-errCtx.errCh:
			}
		}
		return children
	}
}

func writeErr(err error, ctx *errContext) {
	ctx.err = err
	close(ctx.errCh)
}

func transform[T any](fileSystem fs.FileSystem, errCtx *errContext, once *sync.Once) func(file file) T {
	return func(file file) T {
		defer catchPanic(once, errCtx)
		f, err := fileSystem.Open(file.name)
		if err != nil {
			once.Do(func() { writeErr(err, errCtx) })
		}
		defer f.Close()
		var res T
		decoder := json.NewDecoder(f)
		err = decoder.Decode(&res)
		if err != nil {
			once.Do(func() { writeErr(err, errCtx) })
		}
		return res
	}
}

func (c *crawlerImpl[T, R]) Collect(
	ctx context.Context,
	fileSystem fs.FileSystem,
	root string,
	conf Configuration,
	accumulator workerpool.Accumulator[T, R],
	combiner Combiner[R],
) (R, error) {
	wp := workerpool.New[T, R]()
	wpFile := workerpool.New[file, T]()

	files := make(chan file)
	var once sync.Once

	errCtx := &errContext{
		errCh: make(chan error),
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		wpFile.List(ctx, conf.SearchWorkers, file{root, true}, searcher(fileSystem, files, errCtx, &once))
		close(files)
	}()

	transformedFiles := wpFile.Transform(ctx, conf.FileWorkers, files, transform[T](fileSystem, errCtx, &once))
	accumValues := wp.Accumulate(ctx, conf.AccumulatorWorkers, transformedFiles, accumulator)

	var res R
	for value := range accumValues {
		res = combiner(res, value)
	}
	wg.Wait()

	select {
	case <-ctx.Done():
		close(errCtx.errCh)
		return res, ctx.Err()
	case <-errCtx.errCh:
		return res, errCtx.err
	default:
		return res, nil
	}
}
