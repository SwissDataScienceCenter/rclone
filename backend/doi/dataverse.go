package doi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

func resolveDataverseEndpoint(resolvedURL *url.URL) (provider Provider, endpoint *url.URL, err error) {
	fs.Logf(nil, "dataverseURL = %s", resolvedURL.String())

	queryValues := resolvedURL.Query()
	persistentId := queryValues.Get("persistentId")

	fs.Logf(nil, "persistentId = %s", persistentId)

	query := url.Values{}
	query.Add("persistentId", persistentId)
	endpointURL := resolvedURL.ResolveReference(&url.URL{Path: "/api/datasets/:persistentId/", RawQuery: query.Encode()})

	fs.Logf(nil, "endpointURL = %s", endpointURL)
	return Dataverse, endpointURL, err
}

func (f *Fs) listDataverse(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	// TODO: support subfolders (`directoryLabel`)
	if dir != "" {
		err := fmt.Errorf("doi remote does not support subfolders")
		return nil, fmt.Errorf("error listing %q: %w", dir, err)
	}

	fileEntries, err := f.listDataverseDoiFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing %q: %w", dir, err)
	}
	for _, entry := range fileEntries {
		entries = append(entries, entry)
	}
	return entries, nil
}

// List the files contained in the DOI
func (f *Fs) listDataverseDoiFiles(ctx context.Context) (entries []*Object, err error) {
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
		record := new(dataverseDataset)
		err = rest.DecodeJSON(res, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to readDir: %w", err)
		}
		modTime, modTimeErr := time.Parse(time.RFC3339, record.Data.LatestVersion.LastUpdateTime)
		if modTimeErr != nil {
			fs.Logf(f, "rrror: could not parse last update time %v", modTimeErr)
			modTime = timeUnset
		}
		for _, file := range record.Data.LatestVersion.Files {
			path := fmt.Sprintf("/api/access/datafile/%d", file.DataFile.ID)
			contentURL := f.endpoint.ResolveReference(&url.URL{Path: path})
			entry := &Object{
				fs:          f,
				name:        file.DataFile.Filename,
				contentURL:  contentURL.String(),
				size:        file.DataFile.Size,
				modTime:     modTime,
				md5:         file.DataFile.MD5,
				contentType: file.DataFile.ContentType,
			}
			entries = append(entries, entry)
		}
	default:
		return nil, fmt.Errorf("can't parse content type %q", contentType)
	}

	return entries, nil
}

type dataverseDataset struct {
	Data dataverseDatasetData `json:"data"`
}

type dataverseDatasetData struct {
	LatestVersion dataverseDatasetLatestVersion `json:"latestVersion"`
}

type dataverseDatasetLatestVersion struct {
	LastUpdateTime string          `json:"lastUpdateTime"`
	Files          []dataverseFile `json:"files"`
}

type dataverseFile struct {
	DataFile dataverseDataFile `json:"dataFile"`
}

type dataverseDataFile struct {
	ID          int64  `json:"id"`
	Filename    string `json:"filename"`
	ContentType string `json:"contentType"`
	Size        int64  `json:"filesize"`
	MD5         string `json:"md5"`
}
