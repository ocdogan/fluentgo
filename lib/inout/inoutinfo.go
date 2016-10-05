package inout

type InOutInfo struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	IOType      string `json:"iotype,omitempty"`
	Enabled     bool   `json:"enabled"`
	Processing  bool   `json:"processing"`
}
