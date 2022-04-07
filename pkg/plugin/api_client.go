// Copyright (C) 2019-2020 Splunk, Inc. All rights reserved.
package plugin

import (
	"net/http"
	"time"
)

type SignalFxApiCall struct {
	BaseURL string `json:"-"`
	Method  string `json:"-"`
	Token   string `json:"-"`
	Path    string `json:"path"`
	Query   string `json:"query"`
	Data    string `json:"data"`
}

type SignalFxApiClient struct {
	httpClient *http.Client
}

type MetricResponseItem struct {
	Name string `json:"name"`
}

type MetricResponse struct {
	Results []MetricResponseItem `json:"results"`
}

func NewSignalFxApiClient() *SignalFxApiClient {
	client := &SignalFxApiClient{
		httpClient: &http.Client{
			Timeout: time.Second * 30,
		},
	}
	return client
}

// func (t *SignalFxApiClient) doRequest(apiCall *SignalFxApiCall, response interface{}) error {
// 	u, err := url.Parse(apiCall.BaseURL)
// 	if err != nil {
// 		log.DefaultLogger.Error("Error parsing SignalFx API URL", "error", err)
// 		return err
// 	}
// 	u.Path = path.Join(u.Path, apiCall.Path)
// 	u.RawQuery = apiCall.Query
// 	var req *http.Request
// 	if apiCall.Method == http.MethodPost {
// 		req, err = http.NewRequest(http.MethodPost, u.String(), bytes.NewBuffer([]byte(apiCall.Data)))
// 	} else {
// 		req, err = http.NewRequest(http.MethodGet, u.String(), nil)
// 	}
// 	if err != nil {
// 		log.DefaultLogger.Error("Error creating request to SignalFx API", "error", err)
// 		return err
// 	}
// 	req.Header.Set("X-SF-TOKEN", apiCall.Token)
// 	if apiCall.Method == http.MethodPost {
// 		req.Header.Set("Content-Type", "application/json")
// 	}
// 	rsp, err := t.httpClient.Do(req)
// 	if err != nil {
// 		log.DefaultLogger.Error("Error calling SignalFx API", "error", err)
// 		return err
// 	}
// 	defer rsp.Body.Close()
// 	if rsp.StatusCode/100 != 2 {
// 		message, _ := ioutil.ReadAll(rsp.Body)
// 		err = fmt.Errorf("bad status %d: %s", rsp.StatusCode, message)
// 		log.DefaultLogger.Error("Error response from SignalFx API", "error", err)
// 		return err
// 	}

// 	err = json.NewDecoder(rsp.Body).Decode(response)
// 	if err != nil {
// 		log.DefaultLogger.Error("Error decoding response from SignalFx API", "error", err)
// 		return err
// 	}
// 	return nil
// }
