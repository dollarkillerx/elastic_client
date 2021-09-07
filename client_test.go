package simple_elasticsearch_client

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
)

func TestClient(t *testing.T) {
	client := New("127.0.0.1:9200")
	index := client.Index("smoothie")

	exec, err := index.OrgSearch(`{"query" : {"bool" : {"must" : [{"match_phrase" : {"kw.enterprise_group_id" : {"query" : "853771"}}}]}},"from" : 0,"size" : 1}`)
	if err != nil {
		log.Fatalln(err)
	}

	resp, err := exec.ToOrgModel()
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(resp.HTTPCode)
	marshal, err := json.Marshal(resp)
	if err == nil {
		fmt.Println(string(marshal))
	}
}

func TestClientCount(t *testing.T) {
	client := New("192.168.88.11:9203")
	count, err := client.Count("theme_v3")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(count)
}

var mapping = `
{
  "mappings": {
    "dynamic_templates": [
      {
        "ik_fields": {
          "path_match": "ik.*",
          "match_mapping_type": "string",
          "mapping": {
            "analyzer": "standard",
            "search_analyzer": "standard",
            "type": "text"
          }
        }
      },
      {
        "keyword_fields": {
          "path_match": "kw.*",
          "match_mapping_type": "string",
          "mapping": {
            "analyzer": "standard",
            "type": "keyword"
          }
        }
      }
    ]
  }
}
`

func TestClientCreateIndex(t *testing.T) {
	client := New("192.168.88.11:9203")
	err := client.CreateIndex("theme_v4", nil)
	if err != nil {
		log.Fatalln(err)
	}
}

func TestClientDelIndex(t *testing.T) {
	client := New("192.168.88.11:9203")
	err := client.DeleteIndex("theme_v3")
	if err != nil {
		log.Fatalln(err)
	}
}
