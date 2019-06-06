package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strings"
)

func ShiftPath(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}

func notFoundHandler(res http.ResponseWriter, req *http.Request) {
	http.Error(res, fmt.Sprintf(`{status: "error", msg: "page not found"}`), http.StatusNotFound)

}

func badRequestHandler(res http.ResponseWriter, req *http.Request, msg string) {
	http.Error(res, fmt.Sprintf(`{status: "error", msg: "%s"}`, msg), http.StatusBadRequest)

}

func booksHandler(res http.ResponseWriter, req *http.Request) {
	_, id := ShiftPath(req.RequestURI)
	id, _ = ShiftPath(id)

	client, ctx := ConnectToDB("http://10.200.10.1:9200/")
	switch req.Method {

	case "GET":
		if result, err := GetBookByID(client, ctx, id); err != nil {
			badRequestHandler(res, req, err.Error())
		} else {
			fmt.Fprintf(res, `{status: "success", res: "%s"}`, result)
		}

	case "PUT":

		// Read body
		reqBody, err := ioutil.ReadAll(req.Body)
		if err != nil {
			badRequestHandler(res, req, err.Error())
			return
		}

		// Add book
		if newID, err := AddBook(client, ctx, id, string(reqBody)); err != nil {
			badRequestHandler(res, req, err.Error())
		} else {
			fmt.Fprintf(res, `{status: "success", res: "%s"}`, newID)
		}

	case "DELETE":
		if err := DeleteBookByID(client, ctx, id); err != nil {
			badRequestHandler(res, req, err.Error())
		} else {
			fmt.Fprintf(res, `{status: "success"}`)
		}
	default:
		badRequestHandler(res, req, "unsupported method")
	}
}

func searchHandler(res http.ResponseWriter, req *http.Request) {

	client, ctx := ConnectToDB("http://10.200.10.1:9200/")
	title, from, to, sortEbook := extractParams(req)

	switch req.Method {

	case "GET":
		if result, err := SearchBook(client, ctx, title, from, to, sortEbook); err != nil {
			badRequestHandler(res, req, err.Error())
		} else {
			fmt.Fprintf(res, `{status: "success", res: "%s"}`, result)
		}

	default:
		badRequestHandler(res, req, "unsupported method")
	}
}

func recentReqsHandler(res http.ResponseWriter, req *http.Request) {

	_, id := ShiftPath(req.RequestURI)
	id, _ = ShiftPath(id)

	switch req.Method {

	case "GET":
		if result, err := GetRecentRequests(id); err != nil {
			badRequestHandler(res, req, err.Error())
		} else {
			fmt.Fprintf(res, `{status: "success", res: "%s"}`, result)
		}

	default:
		badRequestHandler(res, req, "unsupported method")
	}
}

func extractParams(req *http.Request) (title string, from string, to string, sortEbook bool) {

	params, ok := req.URL.Query()["title"]
	if !ok || len(params[0]) < 1 {
		title = ""
	} else {
		title = params[0]
	}

	params, ok = req.URL.Query()["from"]
	if !ok || len(params[0]) < 1 {
		from = ""
	} else {
		from = params[0]
	}

	params, ok = req.URL.Query()["to"]
	if !ok || len(params[0]) < 1 {
		to = ""
	} else {
		to = params[0]
	}

	params, ok = req.URL.Query()["sort_ebook"]
	if !ok || len(params[0]) < 1 {
		sortEbook = false
	} else {
		sortEbook = true
	}
	return title, from, to, sortEbook
}

func main() {
	http.HandleFunc("/book/", booksHandler)
	http.HandleFunc("/recent/", recentReqsHandler)
	http.HandleFunc("/search", searchHandler)
	http.HandleFunc("/", notFoundHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
