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
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/doi/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/cache"
	"github.com/rclone/rclone/lib/rest"
)

// the URL of the DOI resolver
//
// Reference: https://www.doi.org/the-identifier/resources/factsheets/doi-resolution-documentation
const doiResolverApiURL = "https://doi.org/api"

var (
	errorReadOnly = errors.New("doi remotes are read only")
	timeUnset     = time.Unix(0, 0)
)

func init() {
	fsi := &fs.RegInfo{
		Name:        "doi",
		Description: "DOI datasets",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		Options: []fs.Option{{
			Name:     "doi",
			Help:     "The DOI or the doi.org URL.",
			Required: true,
		}, {
			Name: fs.ConfigProvider,
			Help: `DOI provider.

The DOI provider can be set when rclone does not automatically recognize a supported DOI provider.`,
			Examples: []fs.OptionExample{{
				Value: string(Zenodo),
				Help:  "Zenodo",
			}, {
				Value: string(Dataverse),
				Help:  "Dataverse",
			}},
			Required: false,
			Advanced: true,
		}},
	}
	fs.Register(fsi)
}

// Provider defines the type of provider hosting the DOI
type Provider string

var (
	// Zenodo, see https://zenodo.org
	Zenodo Provider = "zenodo"
	// Dataverse, see https://dataverse.harvard.edu
	Dataverse Provider = "dataverse"
)

// Options defines the configuration for this backend
type Options struct {
	Doi      string `config:"doi"`      // The DOI, a digital identifier of an object, usually a dataset
	Provider string `config:"provider"` // The DOI provider
}

// Fs stores the interface to the remote HTTP files
type Fs struct {
	name        string         // name of this remote
	root        string         // the path we are working on
	doi         string         // the parsed DOI
	provider    Provider       // the DOI provider
	features    *fs.Features   // optional features
	opt         Options        // options for this backend
	ci          *fs.ConfigInfo // global config
	endpoint    *url.URL       // the main API endpoint for this remote
	endpointURL string         // endpoint as a string
	srv         *rest.Client   // the connection to the server
	// TODO: add a pacer (from fs) for HTTP requests

	cache *cache.Cache // a cache for the remote metadata
}

// Object is a remote object that has been stat'd (so it exists, but is not necessarily open for reading)
type Object struct {
	fs *Fs // what this object is part of
	// TODO: remove `name` field

	name        string    // the name of the file
	remote      string    // the remote path
	contentURL  string    // the URL where the contents of the file can be downloaded
	size        int64     // size of the object
	modTime     time.Time // modification time of the object
	contentType string    // content type of the object
	md5         string    // MD5 hash of the object content
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

// Parse the input string as a DOI
// Examples:
// 10.1000/182 -> 10.1000/182
// https://doi.org/10.1000/182 -> 10.1000/182
// doi:10.1000/182 -> 10.1000/182
func parseDoi(doi string) string {
	doiURL, err := url.Parse(doi)
	if err != nil {
		return doi
	}
	if doiURL.Scheme == "doi" {
		return strings.TrimLeft(strings.TrimLeft(doi, "doi:"), "/")
	}
	if strings.HasSuffix(doiURL.Hostname(), "doi.org") {
		return strings.TrimLeft(doiURL.Path, "/")
	}
	return doi
}

// Resolve a DOI to a URL
// Reference: https://www.doi.org/the-identifier/resources/factsheets/doi-resolution-documentation
func resolveDoiURL(ctx context.Context, client *http.Client, opt *Options) (doiURL *url.URL, err error) {
	doi := parseDoi(opt.Doi)
	doiRestClient := rest.NewClient(client).SetRoot(doiResolverApiURL)
	params := url.Values{}
	params.Add("index", "1")
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/handles/" + doi,
		Parameters: params,
	}
	var result api.DoiResolverResponse
	_, err = doiRestClient.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		return nil, err
	}

	if result.ResponseCode != 1 {
		return nil, fmt.Errorf("could not resolve DOI (error code %d)", result.ResponseCode)
	}
	resolvedURLStr := ""
	for _, value := range result.Values {
		if value.Type == "URL" && value.Data.Format == "string" {
			valueStr, ok := value.Data.Value.(string)
			if !ok {
				return nil, fmt.Errorf("could not resolve DOI (incorrect response format)")
			}
			resolvedURLStr = valueStr
		}
	}
	resolvedURL, err := url.Parse(resolvedURLStr)
	if err != nil {
		return nil, err
	}
	return resolvedURL, nil
}

// Resolve the passed configuration into a provider and enpoint
func resolveEndpoint(ctx context.Context, client *http.Client, opt *Options) (provider Provider, endpoint *url.URL, err error) {
	resolvedURL, err := resolveDoiURL(ctx, client, opt)
	if err != nil {
		return "", nil, err
	}

	hostname := strings.ToLower(resolvedURL.Hostname())

	// TODO: improve dataverse detection
	if hostname == "dataverse.harvard.edu" || opt.Provider == string(Dataverse) {
		return resolveDataverseEndpoint(resolvedURL)
	}
	if hostname == "zenodo.org" || strings.HasSuffix(hostname, ".zenodo.org") || opt.Provider == string(Zenodo) {
		return resolveZenodoEndpoint(ctx, client, resolvedURL, opt.Doi)
	}

	return "", nil, fmt.Errorf("provider '%s' is not supported", resolvedURL.Hostname())
}

// Make the http connection from the passed options
func (f *Fs) httpConnection(ctx context.Context, opt *Options) (isFile bool, err error) {
	client := fshttp.NewClient(ctx)

	provider, endpoint, err := resolveEndpoint(ctx, client, opt)
	if err != nil {
		return false, err
	}

	// Note that we assume that there are no subfolders for DOI objects
	// isFile = f.root != ""

	// Update f with the new parameters
	f.srv = rest.NewClient(client).SetRoot(endpoint.ResolveReference(&url.URL{Path: "/"}).String())
	f.cache = cache.New()
	f.endpoint = endpoint
	f.endpointURL = endpoint.String()
	f.doi = parseDoi(opt.Doi) // TODO: avoid calling parseDoi() again here
	f.provider = provider

	// Determine if the root is a file
	switch f.provider {
	case Dataverse:
		entries, err := f.listDataverseDoiFiles(ctx)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if entry.remote == f.root {
				isFile = true
				break
			}
		}
	case Zenodo:
		isFile = f.root != ""
	}

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
		// f.root = ""
		newRoot := path.Dir(f.root)
		if newRoot == "." {
			newRoot = ""
		}
		f.root = newRoot
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
	return fmt.Sprintf("DOI %s", f.doi)
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
	var entries []*Object
	var err error
	switch f.provider {
	case Dataverse:
		entries, err = f.listDataverseDoiFiles(ctx)
	case Zenodo:
		entries, err = f.listZenodoDoiFiles(ctx)
	default:
		err = fmt.Errorf("provider type '%s' not supported", f.provider)
	}
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
	switch f.provider {
	case Dataverse:
		return f.listDataverse(ctx, dir)
	case Zenodo:
		return f.listZenodo(ctx, dir)
	default:
		return nil, fmt.Errorf("provider type '%s' not supported", f.provider)
	}
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
	return o.remote
}

// Remote the name of the remote HTTP file, relative to the fs root
func (o *Object) Remote() string {
	return o.remote
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
	opts := rest.Opts{
		Method:  "GET",
		RootURL: o.contentURL,
		Options: options,
	}
	fs.Logf(o, "Open with URL = '%s'", o.contentURL)
	for _, opt := range opts.Options {
		k, v := opt.Header()
		fs.Logf(o, "header '%s' = '%s'", k, v)
	}
	res, err := o.fs.srv.Call(ctx, &opts)
	if err != nil {
		fs.Logf(o, "Open failed: '%s'", res.Status)
		fs.Logf(o, "Open failed: '%s'", err.Error())
		return nil, fmt.Errorf("Open failed: %w", err)
	}
	fs.Logf(o, "Open response: '%s'", res.Status)
	fs.Logf(o, "Open response: '%v'", res.Header)
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

var commandHelp = []fs.CommandHelp{{
	Name:  "show-metadata",
	Short: "Show metadata about the DOI.",
	Long: `This command returns the JSON representation of the DOI.

    rclone backend show-medatadata doi: 

It returns a JSON object representing the DOI.
`,
}, {
	Name:  "set",
	Short: "Set command for updating the config parameters.",
	Long: `This set command can be used to update the config parameters
for a running doi backend.

Usage Examples:

    rclone backend set doi: [-o opt_name=opt_value] [-o opt_name2=opt_value2]
    rclone rc backend/command command=set fs=doi: [-o opt_name=opt_value] [-o opt_name2=opt_value2]
    rclone rc backend/command command=set fs=doi: -o doi=NEW_DOI

The option keys are named as they are in the config file.

This rebuilds the connection to the doi backend when it is called with
the new parameters. Only new parameters need be passed as the values
will default to those currently in use.

It doesn't return anything.
`,
}}

// Command the backend to run a named command
//
// The command run is name
// args may be used to read arguments from
// opts may be used to read optional arguments from
//
// The result should be capable of being JSON encoded
// If it is a string or a []string it will be shown to the user
// otherwise it will be JSON encoded and shown to the user like that
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (out interface{}, err error) {
	switch name {
	case "show-metadata":
		return f.ShowMetadata(ctx)
	case "set":
		newOpt := f.opt
		err := configstruct.Set(configmap.Simple(opt), &newOpt)
		if err != nil {
			return nil, fmt.Errorf("reading config: %w", err)
		}
		_, err = f.httpConnection(ctx, &newOpt)
		if err != nil {
			return nil, fmt.Errorf("updating session: %w", err)
		}
		f.opt = newOpt
		keys := []string{}
		for k := range opt {
			keys = append(keys, k)
		}
		fs.Logf(f, "Updated config values: %s", strings.Join(keys, ", "))
		return nil, nil
	default:
		return nil, fs.ErrorCommandNotFound
	}
}

// ShowMetadata returns the metadata associated with the DOI
func (f *Fs) ShowMetadata(ctx context.Context) (metadata interface{}, err error) {
	metadataURL := f.endpoint
	var result any
	opts := rest.Opts{
		Method:  "GET",
		RootURL: metadataURL.String(),
	}
	_, err = f.srv.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		return nil, err
	}
	return result, err
}

// Check the interfaces are satisfied
var (
	_ fs.Fs          = (*Fs)(nil)
	_ fs.PutStreamer = (*Fs)(nil)
	_ fs.Commander   = (*Fs)(nil)
	_ fs.Object      = (*Object)(nil)
	_ fs.MimeTyper   = (*Object)(nil)
)
