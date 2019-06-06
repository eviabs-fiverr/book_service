package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/redis.v5"
	"time"
)

type Book struct {
	Title          string `json:"title"`
	AuthorName     string `json:"author_name"`
	Price          int    `json:"price"`
	PublishDate    string `json:"publish_date"`
	EbookAvailable bool   `json:"ebook_available"`
}

const mapping = `
{
	"mappings":{
		"book":{
		  "properties": {
			"title": {
			  "type": "text",
			  "fields": {
				"raw": {
				  "type":  "keyword"
				}
			  }
			},
			"author_name": {
			  "type": "text",
			  "fields": {
				"raw": {
				  "type":  "keyword"
				}
			  }
			},
			"price":      { "type": "float" },
			"ebook_available": { "type": "boolean" },
			"publish_date":  { "type":   "date" }
		  }
		}
	}
}`

func ConnectToDB(url string) (*elastic.Client, context.Context) {
	println("connecting...")
	// Starting with elastic.v5, you must pass a context to execute each service
	ctx := context.Background()

	client, err := elastic.NewSimpleClient(elastic.SetURL(url))
	if err != nil {
		println(err.Error())
		return nil, nil
	}

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := client.Ping(url).Do(ctx)
	if err != nil {
		println(err.Error())
		return nil, nil
	}

	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)
	return client, ctx
}

func AddBook(client *elastic.Client, ctx context.Context, bookID string, bookJSON string) (string, error) {

	putBook, err := client.Index().
		Index("books").
		Type("book").
		Id(bookID).
		BodyString(bookJSON).
		Do(ctx)
	if err != nil {
		return "", err
	}

	addUserRequestToRedis("userID", fmt.Sprintf(`Index book to "%s"`, putBook.Id))
	fmt.Printf("Indexed bookJSON %s to index %s, type %s\n", putBook.Id, putBook.Index, putBook.Type)
	return putBook.Id, nil
}

func GetBookByID(client *elastic.Client, ctx context.Context, bookID string) (string, error) {

	getBook, err := client.Get().
		Index("books").
		Type("book").
		Id(bookID).
		Do(ctx)
	if err != nil {
		println(err.Error())
		return string(err.Error()), nil
	}

	if getBook.Found {
		addUserRequestToRedis("userID", fmt.Sprintf(`Retrieve book with id "%s"`, bookID))
		return string(*getBook.Source), nil
	}
	return "book not found", nil

}

func DeleteBookByID(client *elastic.Client, ctx context.Context, bookID string) error {
	res, err := client.Delete().
		Index("books").
		Type("book").
		Id(bookID).
		Do(ctx)
	if err != nil {
		println(err.Error())
		return err
	}
	if res.Found {
		addUserRequestToRedis("userID", fmt.Sprintf(`Delete book with id "%s"`, bookID))
		fmt.Print("Document deleted from from index\n")
		return nil
	}

	return errors.New("book not found")
}

func SearchBook(client *elastic.Client, ctx context.Context, title string, from string, to string, sortEbook bool) (string, error) {

	queries := make([]elastic.Query, 0)

	searchObject := client.Search().Index("books")

	// match
	if title != "" {
		queries = append(queries, elastic.NewMatchQuery("title", title))
	}

	// price range
	if from != "" && to != "" {
		queries = append(queries, elastic.NewBoolQuery().Filter(elastic.NewRangeQuery("price").From(from).To(to)))
	}

	searchObject.Query(elastic.NewBoolQuery().Must(queries...))

	// sort by "ebook" field, ascending
	if sortEbook {
		searchObject.Sort("price", true)
	}

	// pretty print request and response JSON
	searchObject.Pretty(true)

	// execute
	searchResult, err := searchObject.Do(ctx)
	if err != nil {
		println(err.Error())
		return string(err.Error()), nil
	}

	// no results
	if len(searchResult.Hits.Hits) == 0 {
		return "nothing found", nil
	}

	// Iterate through results
	res := ""
	for _, hit := range searchResult.Hits.Hits {
		//res += fmt.Sprintf(` id: "%s" `, hit.Id)
		res += string(*hit.Source)
	}

	addUserRequestToRedis("userID", fmt.Sprintf(`Search for book(s) with title "%s"`, title))

	return res, nil
}

func GetRecentRequests(userID string) ([]string, error) {

	client, err := connectToRedis()

	if userID == "" {
		println("REDIS: empty args for lpush")
		return nil, errors.New("redis error")
	}

	if err != nil {
		println("REDIS: could not connect")
		return nil, errors.New("redis error")
	}

	if res, err := client.ZRevRange(userID, 0, 2).Result(); err == nil {
		return res, nil
	}

	println("could bot add to set")
	return nil, errors.New("redis error")
}

func addUserRequestToRedis(userID, request string) {
	client, err := connectToRedis()

	if userID == "" || request == "" {
		println("REDIS: empty args for lpush")
	}

	if err != nil {
		println("REDIS: could not connect")
	}

	zz := redis.Z{Score: float64(time.Now().UnixNano() / int64(time.Millisecond)), Member: request}

	if err := client.ZAdd(userID, zz); err != nil {
		println("REDIS: could not lpush")
		return
	}

	println("REDIS updated")
}

func connectToRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()

	return client, err
}
