package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
)

type Book struct {
	Title string
	AuthorName string
	Price int
	EbookAvailable bool
	PublishDate string
}

func (book Book) ToJSON() string {
	json := fmt.Sprintf("%s, %s, %s, %s, %s",
		jsonifyString("title", book.Title),
		jsonifyString("author_name", book.AuthorName),
		jsonifyNumber("price", strconv.Itoa(book.Price)),
		jsonifyString("publish_date", book.PublishDate),
		jsonifyNumber("ebook_available", strconv.FormatBool(book.EbookAvailable)))

	return fmt.Sprintf("{%s}\n", json)
}

func randate() string {

	return fmt.Sprintf("%d-0%d-03T%d:36:50Z",
		rand.Intn(100) + 1900,
		rand.Intn(9) + 1,
		rand.Intn(10) + 10)
}

func jsonifyString(name string, value string) string {
	json := fmt.Sprintf("\"%s\" : \"%s\"", name, value)
	return json
}

func jsonifyNumber(name string, value string) string {
	json := fmt.Sprintf("\"%s\" : %s", name, value)
	return json
}

func NewBook(line []string) Book {

	book := Book{}

	book.Title = line[12]
	book.AuthorName = line[1]
	book.EbookAvailable = rand.Float32() < 0.5
	book.Price, _ = strconv.Atoi(line[16])
	book.PublishDate = randate()

	return book

}

func GetJSONFromCSV(filename string) string {

	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}

	json := "POST books/book/_bulk\n"

	// Loop through lines & turn into object
	for _, line := range lines[1:500] {
		data := NewBook(line)

		json += "{\"index\":{}}\n"
		json += data.ToJSON()
	}

	return json
}