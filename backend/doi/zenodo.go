package doi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

// var recordRegex = regexp.MustCompile(`\/records?\/(.+)`)
var zenodoRecordRegex = regexp.MustCompile(`zenodo[.](.+)`)

// Resolve the main API endpoint for a DOI hosted on Zenodo
func resolveZenodoEndpoint(resolvedURL *url.URL, doi string) (provider Provider, endpoint *url.URL, err error) {
	fs.Logf(nil, "zenodoURL = %s", resolvedURL.String())

	match := zenodoRecordRegex.FindStringSubmatch(doi)
	if match != nil {
		recordID := match[1]
		endpointURL := resolvedURL.ResolveReference(&url.URL{Path: "/api/records/" + recordID})

		fs.Logf(nil, "endpointURL = %s", endpointURL.String())
		return Zenodo, endpointURL, nil
	}

	return "", nil, fmt.Errorf("could not derive API endpoint URL from '%s'", resolvedURL.String())
}

// Implements Fs.List() for Zenodo
func (f *Fs) listZenodo(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if dir != "" {
		return nil, fs.ErrorDirNotFound
	}

	fileEntries, err := f.listZenodoDoiFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing %q: %w", dir, err)
	}

	var (
		entriesMu sync.Mutex // to protect entries
		wg        sync.WaitGroup
		checkers  = f.ci.Checkers
		in        = make(chan int, checkers)
	)
	update := func(idx int, fileEntry *Object) {
		entriesMu.Lock()
		fileEntries[idx] = fileEntry
		entriesMu.Unlock()
	}
	for i := 0; i < checkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range in {
				file := fileEntries[idx]
				err := file.head(ctx)
				if err != nil {
					fs.Debugf(file, "skipping because of error: %v", err)
				}
				update(idx, file)
			}
		}()
	}
	for idx := range fileEntries {
		in <- idx
	}
	close(in)
	wg.Wait()
	for _, entry := range fileEntries {
		entries = append(entries, entry)
	}
	return entries, nil
}

// List the files contained in the DOI
func (f *Fs) listZenodoDoiFiles(ctx context.Context) (entries []*Object, err error) {
	URL := f.endpointURL
	// Do the request
	req, err := http.NewRequestWithContext(ctx, "GET", URL, nil)
	if err != nil {
		return nil, fmt.Errorf("readDir failed: %w", err)
	}

	// Manually ask for JSON
	req.Header.Add("Accept", "application/json")

	res, err := f.httpClient.Do(req)
	if err == nil {
		defer fs.CheckClose(res.Body, &err)
		if res.StatusCode == http.StatusNotFound {
			return nil, fs.ErrorDirNotFound
		}
	}
	err = statusError(res, err)
	if err != nil {
		return nil, fmt.Errorf("failed to readDir: %w", err)
	}

	contentType := strings.SplitN(res.Header.Get("Content-Type"), ";", 2)[0]
	switch contentType {
	case "application/json":
		// TODO: split into a parse method?
		record := new(zenodoRecord)
		err = rest.DecodeJSON(res, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to readDir: %w", err)
		}
		for _, file := range record.Files {
			entry := &Object{
				fs:         f,
				name:       file.Key,
				contentURL: file.Links.Self,
				size:       file.Size,
				modTime:    timeUnset,
				md5:        strings.TrimLeft(file.Checksum, "md5:"),
			}
			entries = append(entries, entry)

		}
	default:
		return nil, fmt.Errorf("can't parse content type %q", contentType)
	}

	return entries, nil
}
