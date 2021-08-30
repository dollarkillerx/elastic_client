package simple_elasticsearch_client

import (
	"fmt"

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

func (e *EsClient) Index(idx string) *EsIndex {
	return &EsIndex{client: e, index: idx}
}

func (e *EsClient) getUrl() string {
	return fmt.Sprintf("%s://%s", e.options.schema, e.addr)
}

func (ei *EsIndex) OrgExec(sql string) (*OrgData, error) {
	code, original, err := ei.client.PackagingRequest(urllib.Post(fmt.Sprintf("%s/%s/_search", ei.client.getUrl(), ei.index))).
		SetJson([]byte(sql)).ByteOriginal()
	if err != nil {
		return nil, err
	}

	return &OrgData{Code: code, Byte: original}, nil
}

func (e *EsClient) PackagingRequest(lib *urllib.Urllib) *urllib.Urllib {
	if e.options.alloverTLS {
		lib = lib.AlloverTLS()
	}

	if e.options.auth == Passwd {
		lib = lib.SetAuth(e.options.user, e.options.password)
	}

	return lib
}
