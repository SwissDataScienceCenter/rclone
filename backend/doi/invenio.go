// Implementation for InvenioDRM

package doi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/doi/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

var invenioRecordRegex = regexp.MustCompile(`\/records?\/(.+)`)

// Resolve the main API endpoint for a DOI hosted on an InvenioDRM installation
func resolveInvenioEndpoint(ctx context.Context, client *http.Client, resolvedURL *url.URL) (provider Provider, endpoint *url.URL, err error) {
	fs.Logf(nil, "invenioURL = %s", resolvedURL.String())

	restClient := rest.NewClient(client)
	opts := rest.Opts{
		Method:  "GET",
		RootURL: resolvedURL.String(),
	}
	res, err := restClient.Call(ctx, &opts)
	if err != nil {
		return "", nil, err
	}

	// First, attempt to grab the API URL from the headers
	var linksetURL *url.URL
	links := parseLinkHeader(res.Header.Get("Link"))
	for _, link := range links {
		if link.Rel == "linkset" && link.Type == "application/linkset+json" {
			parsed, err := url.Parse(link.Href)
			if err == nil {
				linksetURL = parsed
				break
			}
		}
	}

	if linksetURL != nil {
		endpoint, err = checkInvenioApiURL(ctx, restClient, linksetURL)
		if err == nil {
			return Invenio, endpoint, nil
		}
		fs.Logf(nil, "using linkset URL failed: %s", err.Error())
	}

	// If there is no linkset header, try to grab the record ID from the URL
	recordID := ""
	resURL := res.Request.URL
	fs.Logf(nil, "resURL = %s", resURL.String())
	match := invenioRecordRegex.FindStringSubmatch(resURL.EscapedPath())
	if match != nil {
		recordID = match[1]
		guessedURL := res.Request.URL.ResolveReference(&url.URL{
			Path: "/api/records/" + recordID,
		})
		endpoint, err = checkInvenioApiURL(ctx, restClient, guessedURL)
		if err == nil {
			return Invenio, endpoint, nil
		}
		fs.Logf(nil, "guessing the URL failed: %s", err.Error())
	}

	return "", nil, fmt.Errorf("could not resolve the Invenio API endpoint for '%s'", resolvedURL.String())
}

func checkInvenioApiURL(ctx context.Context, client *rest.Client, resolvedURL *url.URL) (endpoint *url.URL, err error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: resolvedURL.String(),
	}
	var result api.InvenioRecordResponse
	_, err = client.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		return nil, err
	}
	if result.Links.Self == "" {
		return nil, fmt.Errorf("could not parse API response from '%s'", resolvedURL.String())
	}
	return url.Parse(result.Links.Self)
}

// Implements Fs.List() for Invenio
func (f *Fs) listInvenio(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if dir != "" {
		return nil, fs.ErrorDirNotFound
	}

	fileEntries, err := f.listInvevioDoiFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing %q: %w", dir, err)
	}
	for _, entry := range fileEntries {
		entries = append(entries, entry)
	}
	return entries, nil
}

// List the files contained in the DOI
func (f *Fs) listInvevioDoiFiles(ctx context.Context) (entries []*Object, err error) {
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

	filesURL := f.endpoint.JoinPath("files")
	var result api.InvenioFilesResponse
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
