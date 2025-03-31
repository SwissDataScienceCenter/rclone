// Implementation for Zenodo

package doi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"

	"github.com/rclone/rclone/backend/doi/api"
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

	restClient := rest.NewClient(client)
	var result api.InvenioRecordResponse
	opts := rest.Opts{
		Method:  "GET",
		RootURL: endpointURL.String(),
	}
	_, err = restClient.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		return "", nil, err
	}

	endpointURL, err = url.Parse(result.Links.Self)
	if err != nil {
		return "", nil, err
	}

	fs.Logf(nil, "endpointURL = %s", endpointURL.String())
	return Zenodo, endpointURL, nil
}
