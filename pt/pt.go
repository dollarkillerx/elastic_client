package main

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	esClient "github.com/dollarkillerx/simple_elasticsearch_client"
)

//ESConfig:
//Host: https://10.20.70.25:9200
//User: elastic
//Password: iyZqtIGlYKwixhOcINHeGnXui8B7s7Ce
//ResponseHeaderTimeoutSeconds: 60

var sql = `{
  "size":10000,
  "sort": [
    {
      "publish_time": {
        "order": "desc"
      }
    }
  ],
  "query": {
    "range": {
      "publish_time": {
        "lte": "2021-08-15 15:03:30",
        "gte": "2021-08-01 00:00:00"
      }
    }
  }
}`

type RelatedEntities struct {
	EntityID   string `json:"entity_id"`
	EntityType int    `json:"entity_type"`
	EntityName string `json:"entity_name"`
}

type Thumbnail struct {
	PicName string `json:"pic_name"`
	PicFile string `json:"pic_file"`
}

type News struct {
	ID              string            `json:"id"`
	Title           string            `json:"title"`
	Abstract        string            `json:"abstract"`
	PublishTime     string            `json:"publish_time"`
	Link            string            `json:"link"`
	Thumbnail       []Thumbnail       `json:"thumbnail"`
	RelatedEntities []RelatedEntities `json:"related_entities"`
	Source          string            `json:"source"`
	Content         string            `json:"content"`
	CreateTime      string            `json:"create_time"`
	Operation       string            `json:"operation"`
	Tag             []string          `json:"tag"`
}

func main() {
	client := esClient.New("10.20.70.25:9200", esClient.SetSchema(esClient.HTTPS), esClient.AlloverTLS(),
		esClient.SetPassword("elastic", "iyZqtIGlYKwixhOcINHeGnXui8B7s7Ce"))

	index := client.Index("prod_news_v2")

	resp, err := index.OrgExec(sql)
	if err != nil {
		log.Fatalln(err)
	}

	model, err := resp.ToOrgModel()
	if err != nil {
		log.Fatalln(err)
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewSyncProducer([]string{"192.168.88.11:9082"}, kafkaConfig)
	if err != nil {
		log.Fatalln(err)
	}

	defer producer.Close()

	for _, v := range model.Hits.Hits {
		var n News
		err := json.Unmarshal(v.Source, &n)
		if err != nil {
			log.Fatalln(err)
		}

		r := map[string]interface{}{
			"id":               n.ID,
			"title":            n.Title,
			"abstract":         n.Abstract,
			"publish_date":     n.PublishTime,
			"link":             n.Link,
			"thumbnail":        n.Thumbnail,
			"related_entities": n.RelatedEntities,
			"source":           n.Source,
			"content":          n.Content,
			"operation":        n.Operation,
			"tag":              n.Tag,
		}

		marshal, err := json.Marshal(r)
		if err != nil {
			log.Fatalln(err)
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(n.ID),
			Value: sarama.ByteEncoder(marshal),
			Topic: "raw_news_from_nj_20210830",
		})
		if err != nil {
			log.Println(err)
		}
	}

}
