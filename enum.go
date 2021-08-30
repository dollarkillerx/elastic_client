package simple_elasticsearch_client

import "encoding/json"

type Schema string

const (
	HTTP  Schema = "http"
	HTTPS Schema = "https"
)

type AUTH string

const (
	Passwd AUTH = "passwd"
	NoAuth AUTH = "no_auth"
)

type OrgData struct {
	Byte []byte
	Code int
}

func (o *OrgData) ToOrgModel() (*OrgModel, error) {
	result := OrgModel{}
	err := json.Unmarshal(o.Byte, &result)
	if err != nil {
		return nil, err
	}

	result.HTTPCode = o.Code
	return &result, err
}

func (o *OrgData) ToSimpleModel() (*SimpleModel, error) {
	result := SimpleModel{}
	err := json.Unmarshal(o.Byte, &result)
	if err != nil {
		return nil, err
	}

	result.HTTPCode = o.Code
	return &result, err
}

type OrgModel struct {
	Took     int64  `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   Shards `json:"_shards"`
	Hits     struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []struct {
			Index  string          `json:"_index"`
			Type   string          `json:"_type"`
			ID     string          `json:"_id"`
			Score  float64         `json:"_score"`
			Source json.RawMessage `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
	HTTPCode int
}

type SimpleModel struct {
	Took     int64  `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   Shards `json:"_shards"`
	Hits     struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []struct {
			Index  string                 `json:"_index"`
			Type   string                 `json:"_type"`
			ID     string                 `json:"_id"`
			Score  float64                `json:"_score"`
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
	HTTPCode int
}

type Shards struct {
	Total      int64 `json:"total"`
	Successful int64 `json:"successful"`
	Skipped    int64 `json:"skipped"`
	Failed     int64 `json:"failed"`
}
