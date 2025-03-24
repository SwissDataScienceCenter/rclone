// Package doi provides a filesystem interface for digital objects identified by DOIs.
// See: https://www.doi.org/the-identifier/what-is-a-doi/
package doi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/rest"
)

var (
	errorReadOnly = errors.New("doi remotes are read only")
	timeUnset     = time.Unix(0, 0)
)

func init() {
	fsi := &fs.RegInfo{
		Name:        "doi",
		Description: "DOI datasets",
		NewFs:       NewFs,
		// CommandHelp: commandHelp,
		Options: []fs.Option{{
			Name:     "doi",
			Help:     "The DOI or the doi.org URL.",
			Required: true,
		}, {
			Name: fs.ConfigProvider,
			Help: "DOI provider.",
			Examples: []fs.OptionExample{{
				Value: "Zenodo",
				Help:  "Zenodo",
			}},
			Required: false,
		}},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	Doi      string `config:"doi"`
	Provider string `config:"provider"`
}

// Fs stores the interface to the remote HTTP files
type Fs struct {
	name        string
	root        string
	features    *fs.Features   // optional features
	opt         Options        // options for this backend
	ci          *fs.ConfigInfo // global config
	endpoint    *url.URL
	endpointURL string // endpoint as a string
	httpClient  *http.Client
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs          *Fs
	name        string
	contentURL  string
	size        int64
	modTime     time.Time
	contentType string
	md5         string
}

// statusError returns an error if the response contained an error
func statusError(res *http.Response, err error) error {
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		_ = res.Body.Close()
		return fmt.Errorf("HTTP Error: %s", res.Status)
	}
	return nil
}

type cslData struct {
	URL string `json:"URL"`
}

// Resolve the passed configuration into an enpoint
func resolveEndpoint(ctx context.Context, client *http.Client, opt *Options) (endpoint *url.URL, err error) {
	// TODO: this assumes `opt.Doi` is a pure DOI
	baseURL, err := url.Parse("https://dx.doi.org/")
	if err != nil {
		return nil, err
	}
	doiURL, err := url.JoinPath(baseURL.String(), opt.Doi)
	if err != nil {
		return nil, err
	}
	fs.Logf(nil, "DOI URL = %s", doiURL)
	req, err := http.NewRequestWithContext(ctx, "GET", doiURL, nil)
	if err != nil {
		return nil, err
	}
	// Manually ask for JSON
	req.Header.Add("Accept", "application/vnd.citationstyles.csl+json")
	res, err := client.Do(req)
	if err == nil {
		defer fs.CheckClose(res.Body, &err)
		if res.StatusCode == http.StatusNotFound {
			return nil, fs.ErrorDirNotFound
		}
	}
	err = statusError(res, err)
	if err != nil {
		return nil, err
	}

	var zenodoURL *url.URL
	contentType := strings.SplitN(res.Header.Get("Content-Type"), ";", 2)[0]
	switch contentType {
	case "application/vnd.citationstyles.csl+json":
		// TODO: split into a parse method?
		record := new(cslData)
		err = rest.DecodeJSON(res, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to read DOI: %w", err)
		}
		zenodoURL, err = url.Parse(record.URL)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("can't parse content type %q", contentType)
	}

	hostname := strings.ToLower(zenodoURL.Hostname())
	if hostname != "zenodo.org" || strings.HasSuffix(hostname, ".zenodo.org") {
		return nil, fmt.Errorf("provider '%s' is not supported", zenodoURL.Hostname())
	}

	fs.Logf(nil, "zenodoURL = %s", zenodoURL.String())

	req, err = http.NewRequestWithContext(ctx, "HEAD", zenodoURL.String(), nil)
	if err != nil {
		return nil, err
	}
	res, err = client.Do(req)
	if err == nil {
		defer fs.CheckClose(res.Body, &err)
		if res.StatusCode == http.StatusNotFound {
			return nil, fs.ErrorDirNotFound
		}
	}
	err = statusError(res, err)
	if err != nil {
		return nil, err
	}

	links := parseLinkHeader(res.Header.Get("Link"))
	linksetURL := ""
	for _, link := range links {
		if link.Rel == "linkset" {
			linksetURL = link.Href
		}
	}
	fs.Logf(nil, "linksetURL = %s", linksetURL)
	return url.Parse(linksetURL)
}

// Make the http connection from the passed options
func (f *Fs) httpConnection(ctx context.Context, opt *Options) (isFile bool, err error) {
	client := fshttp.NewClient(ctx)

	endpoint, err := resolveEndpoint(ctx, client, opt)
	if err != nil {
		return false, err
	}

	// Note that we assume that there are no subfolders for DOI objects
	isFile = f.root != ""

	// Update f with the new parameters
	f.httpClient = client
	f.endpoint = endpoint
	f.endpointURL = endpoint.String()
	return isFile, nil
}

// NewFs creates a new Fs object from the name and root. It connects to
// the host specified in the config file.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	fs.Logf(nil, "name = '%s', root = '%s'", name, root)
	root = strings.Trim(root, "/")

	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	ci := fs.GetConfig(ctx)
	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		ci:   ci,
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	fs.Logf(nil, "name = '%s', root = '%s'", name, root)

	isFile, err := f.httpConnection(ctx, opt)
	if err != nil {
		return nil, err
	}

	if isFile {
		// return an error with an fs which points to the parent
		f.root = ""
		return f, fs.ErrorIsFile
	}

	return f, nil
}

// Name returns the configured name of the file system
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root for the filesystem
func (f *Fs) Root() string {
	return f.root
}

// String returns the URL for the filesystem
func (f *Fs) String() string {
	return f.endpointURL
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision is the remote http file system's modtime precision, which we have no way of knowing. We estimate at 1s
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns hash.HashNone to indicate remote hashing is unavailable
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
	// return hash.Set(hash.None)
}

// Mkdir makes the root directory of the Fs object
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// Remove a remote http file object
func (o *Object) Remove(ctx context.Context) error {
	return errorReadOnly
}

// Rmdir removes the root directory of the Fs object
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// NewObject creates a new remote http file object
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Logf(nil, "remote = %s", remote)

	// TODO: Can we avoid listing the files?
	entries, err := f.listDoiFiles(ctx)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.Remote() == remote {
			fs.Logf(nil, "Found: %s -> %s", entry.Remote(), entry.contentURL)
			return entry, nil
		}
	}

	return nil, fs.ErrorObjectNotFound
}

type zenodoRecord struct {
	Files []zenodoDatasetFile `json:"files"`
}

type zenodoDatasetFile struct {
	ID       string      `json:"id"`
	Key      string      `json:"key"`
	Size     int64       `json:"size"`
	Checksum string      `json:"checksum"`
	Links    zenodoLinks `json:"links"`
}

type zenodoLinks struct {
	Self string `json:"self"`
}

// List the files contained in the DOI
func (f *Fs) listDoiFiles(ctx context.Context) (entries []*Object, err error) {
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

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if dir != "" {
		err := fmt.Errorf("doi remote does not support subfolders")
		return nil, fmt.Errorf("error listing %q: %w", dir, err)
	}

	fileEntries, err := f.listDoiFiles(ctx)
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

// Put in to the remote path with the modTime given of the given size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}

// Fs is the filesystem this remote http file object is located within
func (o *Object) Fs() fs.Info {
	return o.fs
}

// String returns the URL to the remote HTTP file
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.name
}

// Remote the name of the remote HTTP file, relative to the fs root
func (o *Object) Remote() string {
	return o.name
}

// Hash returns "" since HTTP (in Go or OpenSSH) doesn't support remote calculation of hashes
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.md5, nil
}

// Size returns the size in bytes of the remote http file
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the remote http file
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification and access time to the specified time
//
// it also updates the info field
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return errorReadOnly
}

// Storable returns whether the remote http file is a regular file (not a directory, symbolic link, block device, character device, named pipe, etc.)
func (o *Object) Storable() bool {
	return true
}

// Open a remote http file object for reading. Seek is supported
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	fs.FixRangeOption(options, o.size)

	url := o.contentURL
	fs.Logf(nil, "Open URL = %s", url)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}

	// Add optional headers
	for k, v := range fs.OpenOptionHeaders(options) {
		fs.Logf(o, "header %s = %s", k, v)
		req.Header.Add(k, v)
	}
	// o.fs.addHeaders(req)

	// Do the request
	res, err := o.fs.httpClient.Do(req)
	err = statusError(res, err)
	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}
	if err = o.decodeMetadata(res); err != nil {
		return nil, fmt.Errorf("decodeMetadata failed: %w", err)
	}
	return res.Body, nil
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errorReadOnly
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.contentType
}

// head sends a HEAD request to update info fields in the Object
func (o *Object) head(ctx context.Context) error {
	url := o.contentURL
	fs.Logf(nil, "HEAD URL = %s", url)
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return fmt.Errorf("stat failed: %w", err)
	}
	res, err := o.fs.httpClient.Do(req)
	if err == nil && res.StatusCode == http.StatusNotFound {
		return fs.ErrorObjectNotFound
	}
	err = statusError(res, err)
	if err != nil {
		return fmt.Errorf("failed to stat: %w", err)
	}
	return o.decodeMetadata(res)
}

// decodeMetadata updates info fields in the Object according to HTTP response headers
func (o *Object) decodeMetadata(res *http.Response) error {
	t, err := http.ParseTime(res.Header.Get("Last-Modified"))
	if err != nil {
		t = timeUnset
	}
	o.modTime = t
	o.contentType = res.Header.Get("Content-Type")
	o.size = rest.ParseSizeFromHeaders(res.Header)
	return nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = &Fs{}
	_ fs.PutStreamer = &Fs{}
	_ fs.Object      = &Object{}
	_ fs.MimeTyper   = &Object{}
	// _ fs.Commander   = &Fs{}
)
