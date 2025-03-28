// Implementation for InvenioDRM

package doi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/rest"
)

var invenioRecordRegex = regexp.MustCompile(`\/records?\/(.+)`)

// Resolve the main API endpoint for a DOI hosted on Zenodo
func resolveInvenioEndpoint(ctx context.Context, client *http.Client, resolvedURL *url.URL, doi string) (provider Provider, endpoint *url.URL, err error) {
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
	match := invenioRecordRegex.FindStringSubmatch(resolvedURL.EscapedPath())
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

	return "", nil, fmt.Errorf("TODO: resolveInvenioEndpoint()")
}

func checkInvenioApiURL(ctx context.Context, client *rest.Client, resolvedURL *url.URL) (endpoint *url.URL, err error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: resolvedURL.String(),
	}
	var result zenodoRecordResponse
	_, err = client.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		return nil, err
	}
	if result.Links.Self == "" {
		return nil, fmt.Errorf("could not parse API response from '%s'", resolvedURL.String())
	}
	return url.Parse(result.Links.Self)
}
