package operations

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

func SetConfig(connector string, config string) (err error) {

	url := os.Getenv("BP_KAFKA_CONNECT_URL") + fmt.Sprint("/connectors/", connector, "/config")

	payload := strings.NewReader(config)

	req, _ := http.NewRequest("PUT", url, payload)

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)
	fmt.Println(res)
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errors.New("HTTP error : " + fmt.Sprint(res))
		return
	}

	body, _ := ioutil.ReadAll(res.Body)
	fmt.Println(string(body))

	return
}

func GetConfig(connector string) (config string, err error) {
	url := os.Getenv("BP_KAFKA_CONNECT_URL") + fmt.Sprint("/connectors/", connector, "/config")

	req, _ := http.NewRequest("GET", url, nil)

	res, _ := http.DefaultClient.Do(req)
	fmt.Println(res)
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	if res.StatusCode != 200 {
		config = ""
		err = errors.New("HTTP error : " + string(body))
		return
	}

	config = string(body)
	return
}
