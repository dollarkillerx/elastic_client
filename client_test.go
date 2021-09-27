package elastic_client

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
	count, err := client.Count("theme_v3", nil)
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

func TestClientInsert(t *testing.T) {
	client := New("192.168.88.11:9203")
	index := client.Index("theme_v3")
	err := index.Insert(`{
	        "kw.id":"dededede2",
	        "kw.entity_type":0,
	        "created_at":0,
	        "updated_at":null,
	        "kw.redirect_theme_id":"2test",
	        "ik.title":"dawdaw",
	        "ik.summary":"ddad",
	        "kw.publish_time":"2021-09-07T14:43:48.942193511+08:00",
	        "kw.picture_url":"de",
	        "kw.topics":[
	            "d"
	        ],
	        "kw.tags":[
	            "bdb"
	        ],
	        "kw.concepts":[
	            "cdec"
	        ],
	        "related_entity":[
	            {
	                "kw.entity_id":"da",
	                "kw.entity_name":"ad",
	                "kw.entity_type":2
	            }
	        ],
	        "related_news":[
	            {
	                "kw.id":"dad",
	                "ik.title":"daw",
	                "ik.summary":"dad",
	                "kw.source":"1",
	                "ni.link":"1",
	                "kw.published_at":"2021-09-07T14:43:48.942193648+08:00",
	                "is_main":true
	            }
	        ]
	    }`)
	if err != nil {
		log.Fatalln(err)
	}
}

func TestClientInsertBatch(t *testing.T) {
	client := New("192.168.88.11:9203")
	index := client.Index("theme_v3")
	err := index.InsertBatch(`{
	        "kw.id":"dededede3",
	        "kw.entity_type":0,
	        "created_at":0,
	        "updated_at":null,
	        "kw.redirect_theme_id":"2test",
	        "ik.title":"dawdaw",
	        "ik.summary":"ddad",
	        "kw.publish_time":"2021-09-07T14:43:48.942193511+08:00",
	        "kw.picture_url":"de",
	        "kw.topics":[
	            "d"
	        ],
	        "kw.tags":[
	            "bdb"
	        ],
	        "kw.concepts":[
	            "cdec"
	        ],
	        "related_entity":[
	            {
	                "kw.entity_id":"da",
	                "kw.entity_name":"ad",
	                "kw.entity_type":2
	            }
	        ],
	        "related_news":[
	            {
	                "kw.id":"dad",
	                "ik.title":"daw",
	                "ik.summary":"dad",
	                "kw.source":"1",
	                "ni.link":"1",
	                "kw.published_at":"2021-09-07T14:43:48.942193648+08:00",
	                "is_main":true
	            }
	        ]
	    }{
	        "kw.id":"dededede4",
	        "kw.entity_type":0,
	        "created_at":0,
	        "updated_at":null,
	        "kw.redirect_theme_id":"2test",
	        "ik.title":"dawdaw",
	        "ik.summary":"ddad",
	        "kw.publish_time":"2021-09-07T14:43:48.942193511+08:00",
	        "kw.picture_url":"de",
	        "kw.topics":[
	            "d"
	        ],
	        "kw.tags":[
	            "bdb"
	        ],
	        "kw.concepts":[
	            "cdec"
	        ],
	        "related_entity":[
	            {
	                "kw.entity_id":"da",
	                "kw.entity_name":"ad",
	                "kw.entity_type":2
	            }
	        ],
	        "related_news":[
	            {
	                "kw.id":"dad",
	                "ik.title":"daw",
	                "ik.summary":"dad",
	                "kw.source":"1",
	                "ni.link":"1",
	                "kw.published_at":"2021-09-07T14:43:48.942193648+08:00",
	                "is_main":true
	            }
	        ]
	    }{
	        "kw.id":"dededede5",
	        "kw.entity_type":0,
	        "created_at":0,
	        "updated_at":null,
	        "kw.redirect_theme_id":"2test",
	        "ik.title":"dawdaw",
	        "ik.summary":"ddad",
	        "kw.publish_time":"2021-09-07T14:43:48.942193511+08:00",
	        "kw.picture_url":"de",
	        "kw.topics":[
	            "d"
	        ],
	        "kw.tags":[
	            "bdb"
	        ],
	        "kw.concepts":[
	            "cdec"
	        ],
	        "related_entity":[
	            {
	                "kw.entity_id":"da",
	                "kw.entity_name":"ad",
	                "kw.entity_type":2
	            }
	        ],
	        "related_news":[
	            {
	                "kw.id":"dad",
	                "ik.title":"daw",
	                "ik.summary":"dad",
	                "kw.source":"1",
	                "ni.link":"1",
	                "kw.published_at":"2021-09-07T14:43:48.942193648+08:00",
	                "is_main":true
	            }
	        ]
	    }`)
	if err != nil {
		log.Fatalln(err)
	}
}

func TestClientDeleteDocument(t *testing.T) {
	client := New("192.168.88.11:9203")
	index := client.Index("theme_v3")
	err := index.DeleteByQuery(`
			{"query" : {"bool" : {"must" : [{"match_phrase" : {"kw.id" : {"query" : "dededede"}}}]}}}
		`)
	if err != nil {
		log.Fatalln(err)
	}
}
