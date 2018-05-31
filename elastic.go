package elastic

import (
	"net/http"
	"github.com/pkg/errors"
	"encoding/json"
	"strconv"
	"io/ioutil"
	"io"
	"strings"
	"fmt"
)

const (
	uriHealth  = "/_cat/health?format=json"
	uriIndices = "/_cat/indices?format=json"
)

type Client struct {
	url  string
	user string
	pass string
}

type header struct {
	Key   string
	Value string
}

type Index struct {
	UUID   string `json:"uuid"`
	Name   string `json:"index"`
	Health string `json:"health"`
	Status string `json:"status"`
	Count  string `json:"docs.Count"`
	Docs   int
}

var standardHeaders = []header{
	{Key: "Content-Type", Value: "application/json"},
}

// NewClient returns a pointer to a new client initialised with user and pass
func NewClient(url, user, pass string) *Client {
	return &Client{
		url:  url,
		user: user,
		pass: pass,
	}
}

// CheckOK tests the connection
func (c *Client) CheckOK() error {
	_, err := c.request("GET", c.url+uriHealth, nil, standardHeaders)
	return err
}

// Indices returns a list of user-created elastic indices - all those that don't have a name starting with a dot.
func (c *Client) Indices() ([]Index, error) {

	xb, err := c.request("GET", c.url+uriIndices, nil, standardHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "NewRequest")
	}

	var xi []Index
	err = json.Unmarshal(xb, &xi)
	if err != nil {
		return nil, errors.Wrap(err, "Unmarshal")
	}

	// Remove indices with . and set Docs int from Count string
	var xi2 []Index
	for _, v := range xi {
		if v.Name[0] != '.' {
			v.Docs, _ = strconv.Atoi(v.Count) // naughty!
			xi2 = append(xi2, v)
		}
	}

	return xi2, nil
}

// CreateIndex adds a new index, name must be lowercase
func (c *Client) CreateIndex(name string) error {
	n := strings.ToLower(name)
	_, err := c.request("PUT", c.url+"/"+n, nil, standardHeaders)
	if err != nil {
		return errors.Wrap(err, "CreateIndex")
	}
	return nil
}

// DeleteIndex deletes an index
func (c *Client) DeleteIndex(name string) error {
	n := strings.ToLower(name)
	_, err := c.request("DELETE", c.url+"/"+n, nil, standardHeaders)
	if err != nil {
		return errors.Wrap(err, "DeleteIndex")
	}
	return nil
}

// IndexDoc adds or updates a document in the specified index. If id is nil then a new record is created with an
// automatically generated uuid, otherwise the doc is added with the specified id, or updated if the id exists.
func (c *Client) IndexDoc(index, id, doc string) error {
	u := c.url + "/" + strings.ToLower(index) + "/_doc/" + id
	b := strings.NewReader(doc)
	_, err := c.request("POST", u, b, standardHeaders)
	if err != nil {
		return errors.Wrap(err, "IndexDoc")
	}
	return nil
}

// UpdateDoc updates one or more fields in an existing document.
// See: https://www.elastic.co/guide/en/elasticsearch/reference/current/_updating_documents.html
func (c *Client) UpdateDoc(index, id, doc string) error {

	if id == "" {
		return errors.New("UpdateDoc - id must be specified")
	}

	body := `{"doc": ` + doc + `}`

	u := c.url + "/" + strings.ToLower(index) + "/_doc/" + id + "/_update"
	b := strings.NewReader(body)
	_, err := c.request("POST", u, b, standardHeaders)
	if err != nil {
		return errors.Wrap(err, "UpdateDoc")
	}

	return nil
}

// DeleteDoc deletes a document from the specified index
func (c *Client) DeleteDoc(index, id string) error {

	if id == "" {
		return errors.New("UpdateDoc - id must be specified")
	}

	u := c.url + "/" + strings.ToLower(index) + "/_doc/" + id
	_, err := c.request("DELETE", u, nil, standardHeaders)
	if err != nil {
		return errors.Wrap(err, "DeleteDoc")
	}

	return nil
}

// QueryDoc looks up a doc in the specified index, by id
func (c *Client) QueryDoc(index, id string) ([]byte, error) {
	u := c.url + "/" + strings.ToLower(index) + "/_doc/" + id
	xb, err := c.request("GET", u, nil, standardHeaders)
	if err != nil {
		return nil, errors.Wrap(err, "QueryDoc")
	}
	return xb, nil
}

// Batch performs a set of actions specified in the document
// The REST API endpoint /_bulk expects the body to be newline-delimited JSON (NDJSON) and
// hence the Content-Type header to be application/x-ndjson
// https://www.elastic.co/guide/en/elasticsearch/reference/6.2/docs-bulk.html
func (c *Client) Batch(index, doc string) ([]byte, error) {

	u := c.url + "/" + strings.ToLower(index) + "/_doc/_bulk"

	headers := []header{
		{Key: "Content-Type", Value: "application/x-ndjson"},
	}

	b := strings.NewReader(doc)

	xb, err := c.request("POST", u, b, headers)
	if err != nil {
		return nil, errors.Wrap(err, "Batch")
	}

	return xb, nil
}

// request makes a request and returns the response body as a []byte
func (c *Client) request(method, url string, body io.Reader, headers []header) ([]byte, error) {

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, errors.Wrap(err, "request")
	}
	req.SetBasicAuth(c.user, c.pass)

	for _, h := range headers {
		req.Header.Add(h.Key, h.Value)
	}
	fmt.Println(req.Header)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "request")
	}
	if res.StatusCode != http.StatusOK {
		return nil, errors.New(http.StatusText(res.StatusCode) + " - " + errReason(res.Body))
	}
	defer res.Body.Close()

	return ioutil.ReadAll(res.Body)
}

// errReason extracts the error reason message from a response body
func errReason(body io.Reader) string {

	xb, _ := ioutil.ReadAll(body)
	fmt.Println(string(xb))

	var r = struct {
		Error struct {
			Reason string `json:"reason"`
		} `json:"error"`
	}{}

	err := json.NewDecoder(body).Decode(&r)
	if err != nil {
		return err.Error()
	}

	return r.Error.Reason
}
