package elastic_test

import (
	"testing"
	"io/ioutil"
	"log"

	"github.com/matryer/is"
	"github.com/mikedonnici/elastic"
)

const (
	url = "http://dummy.host.com"
	user = "dummyUser"
	pass = "dummyPass"
)

var mockResponseJSON = map[string][]byte{
	"health":   {},
	"indices": {},
}


func init() {

	for i := range mockResponseJSON {
		f := i + ".json"
		xb, err := ioutil.ReadFile("testdata/" + f)
		if err != nil {
			log.Fatalf("Cannot read %s\n", f)
		}
		mockResponseJSON[i] = xb
	}
}

func TestIndices(t *testing.T) {
	is := is.New(t)
	e := elastic.NewClient(url, user, pass)
	e.Indices()
	// Expect 2 indices, named articles and resources
	is.Equal(1,1) // Not equal
}


