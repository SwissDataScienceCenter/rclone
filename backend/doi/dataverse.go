// Implementation for Dataverse

package doi

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/doi/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/rest"
)

// Resolve the main API endpoint for a DOI hosted on a Dataverse installation
func resolveDataverseEndpoint(resolvedURL *url.URL) (provider Provider, endpoint *url.URL, err error) {
	fs.Logf(nil, "dataverseURL = %s", resolvedURL.String())

	queryValues := resolvedURL.Query()
	persistentID := queryValues.Get("persistentId")

	fs.Logf(nil, "persistentId = %s", persistentID)

	query := url.Values{}
	query.Add("persistentId", persistentID)
	endpointURL := resolvedURL.ResolveReference(&url.URL{Path: "/api/datasets/:persistentId/", RawQuery: query.Encode()})

	fs.Logf(nil, "endpointURL = %s", endpointURL)
	return Dataverse, endpointURL, nil
}

// Implements Fs.List() for Dataverse installations
func (f *Fs) listDataverse(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fileEntries, err := f.listDataverseDoiFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing %q: %w", dir, err)
	}
	fullDir := strings.Trim(path.Join(f.root, dir), "/")
	dirPaths := map[string]bool{}
	for _, entry := range fileEntries {
		// First, filter out files not in `fullDir`
		fileDir, _ := dircache.SplitPath(entry.remote)
		if !strings.HasPrefix(fileDir, fullDir) {
			continue
		}
		// Then, find entries in subfolers
		remotePath := entry.remote
		if f.root != "" {
			remotePath = strings.TrimLeft(strings.TrimPrefix(remotePath, f.root), "/")
		}
		fileDir, _ = dircache.SplitPath(remotePath)
		if fileDir == dir {
			newEntry := *entry
			newEntry.remote = remotePath
			entries = append(entries, &newEntry)
		} else if !strings.Contains(fileDir, "/") {
			dirPaths[fileDir] = true
		}
	}
	for dirPath := range dirPaths {
		entry := fs.NewDir(path.Join(dir, dirPath), time.Time{})
		entries = append(entries, entry)
	}
	return entries, nil
}

// List the files contained in the DOI
func (f *Fs) listDataverseDoiFiles(ctx context.Context) (entries []*Object, err error) {
	// Use the cache if populated
	cachedEntries, found := f.cache.GetMaybe("files")
	if found {
		parsedEntries, ok := cachedEntries.([]Object)
		if ok {
			for _, entry := range parsedEntries {
				newEntry := entry
				entries = append(entries, &newEntry)
			}
			return entries, nil
		}
	}

	filesURL := f.endpoint
	fs.Logf(f, "filesURL = '%s'", filesURL.String())
	var result api.DataverseDatasetResponse
	opts := rest.Opts{
		Method:     "GET",
		Path:       strings.TrimLeft(filesURL.EscapedPath(), "/"),
		Parameters: filesURL.Query(),
	}
	fs.Logf(f, "filesAPIPath = '%s?%s'", opts.Path, opts.Parameters.Encode())
	res, err := f.srv.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Logf(f, "%s", res.Status)
		return nil, fmt.Errorf("readDir failed: %w", err)
	}
	modTime, modTimeErr := time.Parse(time.RFC3339, result.Data.LatestVersion.LastUpdateTime)
	if modTimeErr != nil {
		fs.Logf(f, "error: could not parse last update time %v", modTimeErr)
		modTime = timeUnset
	}
	for _, file := range result.Data.LatestVersion.Files {
		contentURLPath := fmt.Sprintf("/api/access/datafile/%d", file.DataFile.ID)
		query := url.Values{}
		query.Add("format", "original")
		contentURL := f.endpoint.ResolveReference(&url.URL{Path: contentURLPath, RawQuery: query.Encode()})
		entry := &Object{
			fs:          f,
			remote:      path.Join(file.DirectoryLabel, file.DataFile.Filename),
			contentURL:  contentURL.String(),
			size:        file.DataFile.FileSize,
			modTime:     modTime,
			md5:         file.DataFile.MD5,
			contentType: file.DataFile.ContentType,
		}
		if file.DataFile.OriginalFileName != "" {
			entry.remote = path.Join(file.DirectoryLabel, file.DataFile.OriginalFileName)
			entry.size = file.DataFile.OriginalFileSize
			entry.contentType = file.DataFile.OriginalFileFormat
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
