package operations

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

type StatusResponse struct {
	Name string
	Connector map[string]string
	Tasks []map[string]interface{}
	Type string
}

func GetStatus(connector string) (status string, err error) {
	url := os.Getenv("BP_KAFKA_CONNECT_URL") + fmt.Sprint("/connectors/", connector, "/status")

	req, _ := http.NewRequest("GET", url, nil)

	res, _ := http.DefaultClient.Do(req)
	fmt.Println(res)
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	if res.StatusCode != 200 {
		status = "ERROR"
		err = errors.New("HTTP error : " + string(body))
		return
	}

	statusRes := new(StatusResponse)
	err = json.Unmarshal(body, statusRes)

	status = statusRes.Connector["state"]
	return
}

func SetStatus(connector string, status string) (err error) {

	url := os.Getenv("BP_KAFKA_CONNECT_URL") + fmt.Sprint("/connectors/", connector, "/", status)

	req, _ := http.NewRequest("PUT", url, nil)

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)
	fmt.Println(res)
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	if res.StatusCode != 202 {
		err = errors.New("HTTP error : " + string(body))
		return
	}

	fmt.Println(string(body))

	return
}
