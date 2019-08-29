package operations

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

func PutConnector(connector string, config string) (err error) {

	url := os.Getenv("BP_KAFKA_CONNECT_URL") + "/connectors"

	var configMap map[string]interface{}
	connectorMap := make(map[string]interface{})
	if err := json.Unmarshal([]byte(config), &configMap); err != nil {
		return err
	}
	connectorMap["name"] = connector
	connectorMap["config"] = configMap

	connectByte, err := json.Marshal(connectorMap)

	payload := strings.NewReader(string(connectByte))

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	if res.StatusCode != 201 {
		err = errors.New("HTTP error : " + fmt.Sprint(string(body)))
		return
	}


	statusRes := new(StatusResponse)
	err = json.Unmarshal(body, statusRes)

	fmt.Println(statusRes)

	return
}

func DelConnector(connector string) (err error) {
	url := os.Getenv("BP_KAFKA_CONNECT_URL") + "/connectors/" + connector

	req, _ := http.NewRequest("DELETE", url, nil)

	res, _ := http.DefaultClient.Do(req)
	fmt.Println(res)
	defer res.Body.Close()

	if res.StatusCode != 204 {
		err = errors.New("HTTP error : " + fmt.Sprint(res))
	}


	return
}
