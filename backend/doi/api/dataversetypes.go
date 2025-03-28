// Type definitions specific to Dataverse

package api

// DataverseDatasetResponse is returned by the Dataverse API
type DataverseDatasetResponse struct {
	Status string           `json:"status"`
	Data   DataverseDataset `json:"data"`
}

// Representation of a dataset
type DataverseDataset struct {
	LatestVersion DataverseDatasetVersion `json:"latestVersion"`
}

// Representation of a dataset version
type DataverseDatasetVersion struct {
	LastUpdateTime string          `json:"lastUpdateTime"`
	Files          []DataverseFile `json:"files"`
}

// Representation of a file found in a dataset
type DataverseFile struct {
	DirectoryLabel string            `json:"directoryLabel"`
	DataFile       DataverseDataFile `json:"dataFile"`
}

// File metadata details
type DataverseDataFile struct {
	ID                 int64  `json:"id"`
	Filename           string `json:"filename"`
	ContentType        string `json:"contentType"`
	FileSize           int64  `json:"filesize"`
	OriginalFileFormat string `json:"originalFileFormat"`
	OriginalFileSize   int64  `json:"originalFileSize"`
	OriginalFileName   string `json:"originalFileName"`
	MD5                string `json:"md5"`
}
