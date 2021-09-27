package simple_elasticsearch_client

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dollarkillerx/urllib"
)

type EsClient struct {
	options *Options
	addr    string
}

func New(addr string, option ...SetOption) *EsClient {
	client := EsClient{
		addr:    addr,
		options: defaultOptions(),
	}

	for _, v := range option {
		v(client.options)
	}

	return &client
}

type EsIndex struct {
	client *EsClient
	index  string
}

// Index es index
func (e *EsClient) Index(idx string) *EsIndex {
	return &EsIndex{client: e, index: idx}
}

// getUrl 获取url
func (e *EsClient) getUrl() string {
	if strings.Contains(e.addr, "http") {
		return e.addr
	}
	return fmt.Sprintf("%s://%s", e.options.schema, e.addr)
}

// packagingRequest 包装基础请求
func (e *EsClient) packagingRequest(lib *urllib.Urllib) *urllib.Urllib {
	if e.options.alloverTLS {
		lib = lib.AlloverTLS()
	}

	if e.options.auth == Passwd {
		lib = lib.SetAuth(e.options.user, e.options.password)
	}

	return lib
}

// OrgSearch 原始查询
func (ei *EsIndex) OrgSearch(sql string) (*OrgData, error) {
	code, original, err := ei.client.packagingRequest(urllib.Post(fmt.Sprintf("%s/%s/_search", ei.client.getUrl(), ei.index))).
		SetJson([]byte(sql)).ByteOriginal()
	if err != nil {
		return nil, err
	}

	return &OrgData{Code: code, Byte: original}, nil
}

type countResponse struct {
	Count  int64 `json:"count"`
	Shards struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	EsError
}

// Count 统计INDEX中文档数量
func (e *EsClient) Count(index string, sql *string) (int64, error) {
	sr := e.packagingRequest(urllib.Get(fmt.Sprintf("%s/%s/_count", e.getUrl(), index)))
	if sql != nil {
		sr = sr.SetJson([]byte(*sql))
	}
	code, original, err := sr.ByteOriginal()
	if err != nil {
		return 0, err
	}
	var result countResponse

	err = json.Unmarshal(original, &result)
	if err != nil {
		return 0, err
	}

	if code != 200 {
		return 0, result.ToError()
	}

	return result.Count, nil
}

// GetMapping
func (e *EsClient) GetMapping(index string) (map[string]interface{}, error) {
	code, original, err := e.packagingRequest(urllib.Get(fmt.Sprintf("%s/%s/_mapping", e.getUrl(), index))).ByteOriginal()
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}

	err = json.Unmarshal(original, &result)
	if err != nil {
		return nil, err
	}

	if code != 200 {
		return nil, errors.New(fmt.Sprintf("error code: %d error: %s", code, string(original)))
	}

	return result, nil
}

type createIndex struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index"`
	EsError
}

// CreateIndex 创建index
func (e *EsClient) CreateIndex(index string, mapping *string) error {
	request := e.packagingRequest(urllib.Put(fmt.Sprintf("%s/%s", e.getUrl(), index)))
	if mapping != nil {
		request = request.SetJson([]byte(*mapping))
	}

	_, bytes, err := request.ByteOriginal()
	if err != nil {
		return err
	}

	var result createIndex
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return err
	}

	if result.Acknowledged == false {
		return result.ToError()
	}

	return nil
}

// DeleteIndex 删除index
func (e *EsClient) DeleteIndex(index string) error {
	request := e.packagingRequest(urllib.Delete(fmt.Sprintf("%s/%s", e.getUrl(), index)))

	_, bytes, err := request.ByteOriginal()
	if err != nil {
		return err
	}

	var result createIndex
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return err
	}

	if result.Acknowledged == false {
		return result.ToError()
	}

	return nil
}

type insertResp struct {
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	Id      string `json:"_id"`
	Version int    `json:"_version"`
	Result  string `json:"result"`
	Shards  struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	SeqNo       int `json:"_seq_no"`
	PrimaryTerm int `json:"_primary_term"`
	EsError
}

// Insert 插入数据
func (ei *EsIndex) Insert(data string) error {
	request := ei.client.packagingRequest(urllib.Post(fmt.Sprintf("%s/%s/_doc", ei.client.getUrl(), ei.index))).SetJson([]byte(data))

	_, bytes, err := request.ByteOriginal()
	if err != nil {
		return err
	}

	var result insertResp
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return err
	}

	if result.Error.ResourceType != "" {
		return result.ToError()
	}

	return nil
}

// InsertBatch  批量插入数据
func (ei *EsIndex) InsertBatch(data string) error {
	request := ei.client.packagingRequest(urllib.Post(fmt.Sprintf("%s/%s/_doc/_bulk", ei.client.getUrl(), ei.index))).SetJson([]byte(data))

	_, bytes, err := request.ByteOriginal()
	if err != nil {
		return err
	}

	var result insertResp
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return err
	}

	if result.Error.ResourceType != "" {
		return result.ToError()
	}

	return nil
}

// DeleteByQuery 通过查询删除数据
func (ei *EsIndex) DeleteByQuery(data string) error {
	request := ei.client.packagingRequest(urllib.Post(fmt.Sprintf("%s/%s/_delete_by_query", ei.client.getUrl(), ei.index))).SetJson([]byte(data))

	_, bytes, err := request.ByteOriginal()
	if err != nil {
		return err
	}

	var result insertResp
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return err
	}

	if result.Error.ResourceType != "" {
		return result.ToError()
	}

	return nil
}
