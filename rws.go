package srctype

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

// NewRwsConn read remote csv file
func NewRwsConn(apiUrl, user, passwd string, proxyUrl string) (Connector, error) {
	bodyBytes, err := httpConn(apiUrl, user, passwd, proxyUrl)
	if err != nil {
		return nil, err
	}

	rawCsv := string(bodyBytes)
	if rawCsv[len(rawCsv)-4:len(rawCsv)] == "EOF" {
		return NewCsvStrConn(rawCsv[0 : len(rawCsv)-4])
	}
	return NewCsvStrConn(rawCsv)
}

// httpConn read remote url
func httpConn(apiUrl, user, passwd string, proxyUrl string) ([]byte, error) {
	var err error

	// read url content
	var client = &http.Client{}
	if proxyUrl != "" {
		proxy, _ := url.Parse(proxyUrl)
		tr := &http.Transport{
			Proxy:           http.ProxyURL(proxy),
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client = &http.Client{
			Transport: tr,
		}
	}

	//read body string
	req, err := http.NewRequest("GET", apiUrl, nil)
	req.SetBasicAuth(user, passwd)
	resp, err := client.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s", resp.Status)
	}

	// Retrieve the body of the response
	return ioutil.ReadAll(resp.Body)
}
