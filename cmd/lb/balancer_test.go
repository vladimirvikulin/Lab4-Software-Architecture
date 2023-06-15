package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestBalancer(c *C) {
	// TODO: Реалізуйте юніт-тест для балансувальникка.
	address1 := getIndex("127.0.0.1:8080")
	address2 := getIndex("192.168.0.0:80")
	address3 := getIndex("26.143.218.9:80")

	c.Assert(address1, Equals, 2)
	c.Assert(address2, Equals, 0)
	c.Assert(address3, Equals, 1)
}

func (s *TestSuite) TestHealth(c *C) {
	result := make([]string, len(serversPool))

	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server2.Close()

	parsedURL1, _ := url.Parse(server1.URL)
	hostURL1 := parsedURL1.Host

	parsedURL2, _ := url.Parse(server2.URL)
	hostURL2 := parsedURL2.Host

	servers := []string{
		hostURL1,
		hostURL2,
		"server3:8080",
	}

	healthCheck(servers, result)
	time.Sleep(12 * time.Second)

	server1.Close()
	time.Sleep(12 * time.Second)

	// Assert that server1 is no longer considered healthy
	c.Assert(result[0], Equals, "")

	// Assert that server2 is still considered healthy
	c.Assert(result[1], Equals, hostURL2)
	c.Assert(result[2], Equals, "")
}
