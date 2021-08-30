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

	exec, err := index.OrgExec(`{"query" : {"bool" : {"must" : [{"match_phrase" : {"kw.enterprise_group_id" : {"query" : "853771"}}}]}},"from" : 0,"size" : 1}`)
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
