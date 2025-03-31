// Type definitions specific to InvenioDRM

package api

// Representation of a record stored in InvenioDRM
type InvenioRecordResponse struct {
	Links InvenioRecordResponseLinks `json:"links"`
}

// Representation of a record's links
type InvenioRecordResponseLinks struct {
	Self string `json:"self"`
}

// Representation of a record's files
type InvenioFilesResponse struct {
	Entries []InvenioFilesResponseEntry `json:"entries"`
}

// Representation of a file entry
type InvenioFilesResponseEntry struct {
	Key      string                         `json:"key"`
	Checksum string                         `json:"checksum"`
	Size     int64                          `json:"size"`
	Updated  string                         `json:"updated"`
	MimeType string                         `json:"mimetype"`
	Links    InvenioFilesResponseEntryLinks `json:"links"`
}

// File links details
type InvenioFilesResponseEntryLinks struct {
	Content string `json:"content"`
}
