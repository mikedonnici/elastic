package elastic

import (
	"net/http"
	"github.com/pkg/errors"
	"encoding/json"
	"strconv"
	"io/ioutil"
	"io"
	"strings"
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

type Index struct {
	UUID   string `json:"uuid"`
	Name   string `json:"index"`
	Health string `json:"health"`
	Status string `json:"status"`
	Count  string `json:"docs.Count"`
	Docs   int
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
	_, err := c.request("GET", c.url+uriHealth, nil)
	return err
}

// Indices returns a list of user-created elastic indices - all those that don't have a name starting with a dot.
func (c *Client) Indices() ([]Index, error) {

	xb, err := c.request("GET", c.url+uriIndices, nil)
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
	_, err := c.request("PUT", c.url+"/"+n, nil)
	if err != nil {
		return errors.Wrap(err, "CreateIndex")
	}
	return nil
}




// request makes a request and returns the response body as a []byte
func (c *Client) request(method, url string, body io.Reader) ([]byte, error) {

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, errors.Wrap(err, "request")
	}
	req.SetBasicAuth(c.user, c.pass)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "Request")
	}
	//if res.StatusCode >= 400 && res.StatusCode <= 500 {
	//	return nil, errors.New(http.StatusText(res.StatusCode) + " " + res.)
	//}
	if res.StatusCode != http.StatusOK {
		return nil, errors.New(http.StatusText(res.StatusCode) + " - " + errReason(res.Body))
	}
	defer res.Body.Close()

	return ioutil.ReadAll(res.Body)
}

// errReason extracts the error reason message from a response body
func errReason(body io.Reader) string {

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
