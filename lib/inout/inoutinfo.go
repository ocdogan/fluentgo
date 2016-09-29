package inout

type InOutInfo struct {
	ID         string `json:"id,omitempty"`
	IOType     string `json:"iotype,omitempty"`
	Enabled    bool   `json:"enabled"`
	Processing bool   `json:"processing"`
}
