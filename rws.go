package srctype

import (
	"crypto/tls"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// NewRwsConn read remote csv file
func NewRwsConn(apiUrl, user, passwd string, proxyUrl string) (Connector, error) {
	bodyBytes, err := httpConn(apiUrl, user, passwd, proxyUrl)
	if err != nil {
		return nil, err
	}

	rawCsv := string(bodyBytes)
	return NewCsvStrConn(rawCsv[0 : len(rawCsv)-4])
}

// httpConn read remote url
func httpConn(apiUrl, user, passwd string, proxyUrl string) ([]byte, error) {
	var err error

	//check url ended with csv or not
	fileExtList := strings.Split(apiUrl, ".")
	fileExt := fileExtList[len(fileExtList)-1]
	if strings.ToLower(fileExt) != "csv" {
		return nil, errors.New("Invalid RWS csv url!")
	}

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

	// Retrieve the body of the response
	return ioutil.ReadAll(resp.Body)
}
