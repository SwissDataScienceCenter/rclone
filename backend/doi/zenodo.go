package doi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

var zenodoRecordRegex = regexp.MustCompile(`zenodo[.](.+)`)

// Resolve the main API endpoint for a DOI hosted on Zenodo
func resolveZenodoEndpoint(ctx context.Context, client *http.Client, resolvedURL *url.URL, doi string) (provider Provider, endpoint *url.URL, err error) {
	fs.Logf(nil, "zenodoURL = %s", resolvedURL.String())

	match := zenodoRecordRegex.FindStringSubmatch(doi)
	if match == nil {
		return "", nil, fmt.Errorf("could not derive API endpoint URL from '%s'", resolvedURL.String())
	}

	recordID := match[1]
	endpointURL := resolvedURL.ResolveReference(&url.URL{Path: "/api/records/" + recordID})

	// Do the request
	req, err := http.NewRequestWithContext(ctx, "GET", endpointURL.String(), nil)
	if err != nil {
		return "", nil, err
	}

	// Manually ask for JSON
	req.Header.Add("Accept", "application/json")

	res, err := client.Do(req)
	if err == nil {
		defer fs.CheckClose(res.Body, &err)
		if res.StatusCode == http.StatusNotFound {
			return "", nil, err
		}
	}
	err = statusError(res, err)
	if err != nil {
		return "", nil, err
	}

	contentType := strings.SplitN(res.Header.Get("Content-Type"), ";", 2)[0]
	switch contentType {
	case "application/json":
		// TODO: split into a parse method?
		record := new(zenodoRecordResponse)
		err = rest.DecodeJSON(res, &record)
		if err != nil {
			return "", nil, err
		}
		endpointURL, err = url.Parse(record.Links.Self)
		if err != nil {
			return "", nil, err
		}
	default:
		return "", nil, fmt.Errorf("can't parse content type %q", contentType)
	}

	fs.Logf(nil, "endpointURL = %s", endpointURL.String())
	return Zenodo, endpointURL, nil
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
	for _, entry := range fileEntries {
		entries = append(entries, entry)
	}
	return entries, nil

	// var (
	// 	entriesMu sync.Mutex // to protect entries
	// 	wg        sync.WaitGroup
	// 	checkers  = f.ci.Checkers
	// 	in        = make(chan int, checkers)
	// )
	// update := func(idx int, fileEntry *Object) {
	// 	entriesMu.Lock()
	// 	fileEntries[idx] = fileEntry
	// 	entriesMu.Unlock()
	// }
	// for i := 0; i < checkers; i++ {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		for idx := range in {
	// 			file := fileEntries[idx]
	// 			err := file.head(ctx)
	// 			if err != nil {
	// 				fs.Debugf(file, "skipping because of error: %v", err)
	// 			}
	// 			update(idx, file)
	// 		}
	// 	}()
	// }
	// for idx := range fileEntries {
	// 	in <- idx
	// }
	// close(in)
	// wg.Wait()
	// for _, entry := range fileEntries {
	// 	entries = append(entries, entry)
	// }
	// return entries, nil
}

// List the files contained in the DOI
func (f *Fs) listZenodoDoiFiles(ctx context.Context) (entries []*Object, err error) {
	filesURL := f.endpoint.JoinPath("files")
	// Do the request
	fs.Logf(f, "filesURL = '%s'", filesURL.String())
	req, err := http.NewRequestWithContext(ctx, "GET", filesURL.String(), nil)
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
		record := new(zenodoFilesResponse)
		err = rest.DecodeJSON(res, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to readDir: %w", err)
		}
		for _, file := range record.Entries {
			modTime, modTimeErr := time.Parse(time.RFC3339, file.Updated)
			if modTimeErr != nil {
				fs.Logf(f, "rrror: could not parse last update time %v", modTimeErr)
				modTime = timeUnset
			}
			entry := &Object{
				fs:          f,
				name:        file.Key,
				contentURL:  file.Links.Content,
				size:        file.Size,
				modTime:     modTime,
				contentType: file.MimeType,
				md5:         strings.TrimLeft(file.Checksum, "md5:"),
			}
			entries = append(entries, entry)
		}
	default:
		return nil, fmt.Errorf("can't parse content type %q", contentType)
	}

	return entries, nil
}

type zenodoRecordResponse struct {
	Links zenodoRecordResponseLinks `json:"links"`
}

type zenodoRecordResponseLinks struct {
	Self string `json:"self"`
}

type zenodoFilesResponse struct {
	Entries []zenodoFilesResponseEntry `json:"entries"`
}

type zenodoFilesResponseEntry struct {
	Key      string                        `json:"key"`
	Checksum string                        `json:"checksum"`
	Size     int64                         `json:"size"`
	Updated  string                        `json:"updated"`
	MimeType string                        `json:"mimetype"`
	Links    zenodoFilesResponseEntryLinks `json:"links"`
}

type zenodoFilesResponseEntryLinks struct {
	Content string `json:"content"`
}
