package doi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLinkHeader(t *testing.T) {
	header := "<https://zenodo.org/api/records/15063252> ; rel=\"linkset\" ; type=\"application/linkset+json\""
	links := parseLinkHeader(header)
	expected := headerLink{
		Href:   "https://zenodo.org/api/records/15063252",
		Rel:    "linkset",
		Type:   "application/linkset+json",
		Extras: map[string]string{},
	}
	assert.Contains(t, links, expected)
}
