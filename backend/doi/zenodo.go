// Implementation for Zenodo

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

	// TODO: use the rest client

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
}

// List the files contained in the DOI
func (f *Fs) listZenodoDoiFiles(ctx context.Context) (entries []*Object, err error) {
	// Use the cache if populated
	cachedEntries, found := f.cache.GetMaybe("files")
	if found {
		fs.Logf(f, "cache hit")
		parsedEntries, ok := cachedEntries.([]Object)
		if ok {
			for _, entry := range parsedEntries {
				entries = append(entries, &entry)
			}
			return entries, nil
		}
	}

	filesURL := f.endpoint.JoinPath("files")
	// Do the request
	// fs.Logf(f, "filesURL = '%s'", filesURL.String())
	var result zenodoFilesResponse
	opts := rest.Opts{
		Method: "GET",
		Path:   strings.TrimLeft(filesURL.EscapedPath(), "/"),
	}
	fs.Logf(f, "filesAPIPath = '%s'", opts.Path)
	_, err = f.srv.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		return nil, fmt.Errorf("readDir failed: %w", err)
	}
	for _, file := range result.Entries {
		modTime, modTimeErr := time.Parse(time.RFC3339, file.Updated)
		if modTimeErr != nil {
			fs.Logf(f, "error: could not parse last update time %v", modTimeErr)
			modTime = timeUnset
		}
		entry := &Object{
			fs:          f,
			name:        file.Key,
			remote:      file.Key,
			contentURL:  file.Links.Content,
			size:        file.Size,
			modTime:     modTime,
			contentType: file.MimeType,
			md5:         strings.TrimLeft(file.Checksum, "md5:"),
		}
		entries = append(entries, entry)
	}
	// Populate the cache
	cacheEntries := []Object{}
	for _, entry := range entries {
		cacheEntries = append(cacheEntries, *entry)
	}
	f.cache.Put("files", cacheEntries)
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
