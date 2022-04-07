// Copyright (C) 2019-2020 Splunk, Inc. All rights reserved.
package plugin

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/signalfx/signalfx-go/signalflow"
	"golang.org/x/net/context"
)

type SignalFxDatasource struct {
	handlers     []SignalFxJob
	client       *signalflow.Client
	url          string
	token        string
	handlerMutex sync.Mutex
	clientMutex  sync.Mutex
	apiClient    *SignalFxApiClient
}

type SignalFxDatasourceSettings struct {
	Url string `json:"url,omitempty"`
}

type Target struct {
	RefID         string        `json:"refId"`
	Program       string        `json:"program"`
	StartTime     time.Time     `json:"-"`
	StopTime      time.Time     `json:"-"`
	Interval      time.Duration `json:"-"`
	Alias         string        `json:"alias"`
	MaxDelay      int64         `json:"maxDelay"`
	MinResolution int64         `json:"minResolution"`
}

func NewSignalFxDatasource(settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var s SignalFxDatasourceSettings
	err := json.Unmarshal(settings.JSONData, &s)
	if err != nil {
		log.DefaultLogger.Error("failed to unmarshal", "err", err, "data", settings.JSONData)
		return nil, err
	}
	url, err := s.buildSignalflowURL()
	if err != nil {
		log.DefaultLogger.Error("failed to parse sfx url", "err", err, "settings", s)
		return nil, err
	}
	accessToken := settings.DecryptedSecureJSONData["accessToken"]
	client, err := createSignalflowClient(url, accessToken)
	if err != nil {
		log.DefaultLogger.Error("failed to create signalflow client", "err", err)
		return nil, err
	}
	datasource := &SignalFxDatasource{
		handlers:     make([]SignalFxJob, 0),
		url:          url,
		token:        accessToken,
		clientMutex:  sync.Mutex{},
		handlerMutex: sync.Mutex{},
		client:       client,
		apiClient:    NewSignalFxApiClient(),
	}
	tick := time.NewTicker(time.Second * 30)
	go datasource.cleanup(tick)
	return datasource, nil
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *SignalFxDatasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Info("CheckHealth called", "request", req)

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func (t *SignalFxDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// create response struct
	response := backend.NewQueryDataResponse()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res := t.query(ctx, req.PluginContext, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}
	return response, nil
}

func (t *SignalFxDatasource) query(_ context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	response := backend.DataResponse{}

	log.DefaultLogger.Debug("Running query", "query", query)

	var apiCall SignalFxApiCall
	response.Error = json.Unmarshal(query.JSON, &apiCall)
	if response.Error != nil {
		return response
	}
	switch apiCall.Path {
	case "/v2/metric":
		apiCall.Method = http.MethodGet
		//response.Frames, response.Error = t.getMetrics(&apiCall)
	case "/v2/suggest/_signalflowsuggest":
		apiCall.Method = http.MethodPost
		//response.Frames, response.Error = t.getSuggestions(&apiCall)
	case "not a real thing":
		response = t.getDatapoints(&query)
	default:
		response = t.getDatapoints(&query)
	}
	log.DefaultLogger.Debug("Finished running query")
	return response
}

// func (t *SignalFxDatasource) getMetrics(tsdbReq *backend.QueryDataRequest, apiCall *SignalFxApiCall) (*backend.QueryDataResponse, error) {
// 	response := MetricResponse{}
// 	t.makeAPICall(tsdbReq, apiCall, &response)
// 	log.DefaultLogger.Debug("Unmarshalled API response", "response", response)
// 	values := make([]string, 0)
// 	for _, r := range response.Results {
// 		values = append(values, r.Name)
// 	}
// 	return t.formatAsTable(values), nil
// }

// func (t *SignalFxDatasource) getSuggestions(tsdbReq *backend.DataQuery, apiCall *SignalFxApiCall) (*backend.QueryDataResponse, error) {
// 	response := make([]string, 0)
// 	t.makeAPICall(tsdbReq, apiCall, &response)
// 	log.DefaultLogger.Debug("Unmarshalled API response", "response", response)
// 	return t.formatAsTable(response), nil
// }

// func (t *SignalFxDatasource) formatAsTable(values []string) *datasource.DatasourceResponse {
// 	table := &datasource.Table{
// 		Columns: make([]*datasource.TableColumn, 0),
// 		Rows:    make([]*datasource.TableRow, 0),
// 	}
// 	table.Columns = append(table.Columns, &datasource.TableColumn{Name: "name"})
// 	for _, r := range values {
// 		row := &datasource.TableRow{}
// 		row.Values = append(row.Values, &datasource.RowValue{Kind: datasource.RowValue_TYPE_STRING, StringValue: r})
// 		table.Rows = append(table.Rows, row)
// 	}
// 	return &datasource.DatasourceResponse{
// 		Results: []*datasource.QueryResult{
// 			{
// 				RefId:  "items",
// 				Tables: []*datasource.Table{table},
// 			},
// 		},
// 	}
// }

// func (t *SignalFxDatasource) makeAPICall(apiCall *SignalFxApiCall, response interface{}) error {
// 	apiCall.BaseURL = t.url
// 	apiCall.Token = t.token
// 	log.DefaultLogger.Debug("Making API Call", "call", apiCall)
// 	return t.apiClient.doRequest(apiCall, &response)
// }

func (t *SignalFxDatasource) getDatapoints(query *backend.DataQuery) backend.DataResponse {
	response := backend.DataResponse{}
	log.DefaultLogger.Info("getDatapoints", "query", query, "token", t.token, "client", t.client, "url", t.url)
	target, err := t.buildTargets(query)
	if err != nil {
		log.DefaultLogger.Error("Could not parse queries", "error", err)
		response.Error = err
		return response
	}
	log.DefaultLogger.Info("starting job handler")
	ch, err := t.startJobHandler(target)
	if err != nil {
		log.DefaultLogger.Error("Could not execute request", "error", err)
		response.Error = err
		return response
	}
	log.DefaultLogger.Info("getting frames")
	frames := <-ch
	// log.DefaultLogger.Info("getting frames")
	// comp, err := t.client.Execute(&signalflow.ExecuteRequest{
	// 	Program: target.Program,
	// 	Start:   target.StartTime,
	// 	Stop:    target.StopTime,
	// })
	// if err != nil {
	// 	log.DefaultLogger.Error("signalfx execution failed", "error", err)
	// 	response.Error = err
	// 	return response
	// }
	// for msg := range comp.Data() {
	// 	// This will run as long as there is data, or until the websocket gets
	// 	// disconnected.
	// 	if len(msg.Payloads) == 0 {
	// 		log.DefaultLogger.Info("No data available")
	// 		continue
	// 	}
	// 	for _, pl := range msg.Payloads {
	// 		meta := comp.TSIDMetadata(pl.TSID)
	// 		log.DefaultLogger.Info("payload data", "metric", meta.OriginatingMetric, "props", meta.CustomProperties, "value", pl.Value())
	// 	}
	// }
	response.Frames = append(response.Frames, frames...)
	return response
}

func createSignalflowClient(url, token string) (*signalflow.Client, error) {
	return signalflow.NewClient(
		signalflow.StreamURL(url),
		signalflow.AccessToken(token),
		signalflow.UserAgent("grafana"))
}

func (s SignalFxDatasourceSettings) buildSignalflowURL() (string, error) {
	return "wss://stream.us0.signalfx.com/v2/signalflow", nil
	// sfxURL, err := url.Parse(s.Url)
	// if err != nil {
	// 	return "", err
	// }
	// scheme := "wss"
	// if sfxURL.Scheme == "http" || sfxURL.Scheme == "" {
	// 	scheme = "ws"
	// }
	// return scheme + "://" + sfxURL.Host + "/v2/signalflow", nil
}

func (t *SignalFxDatasource) startJobHandler(target *Target) (<-chan data.Frames, error) {
	// t.handlerMutex.Lock()
	// // Try to re-use any existing job if possible
	// for _, h := range t.handlers {
	// 	ch := h.reuse(target)
	// 	if ch != nil {
	// 		return ch, nil
	// 	}
	// }
	// t.handlerMutex.Unlock()
	client, err := createSignalflowClient(t.url, t.token)
	if err != nil {
		return nil, err
	}
	handler := &SignalFxJobHandler{
		client: client,
		//FIXME client: t.client,
	}
	return handler.start(target)
	// t.handlerMutex.Lock()
	// if ch != nil {
	// 	t.handlers = append(t.handlers, handler)
	// }
	// t.handlerMutex.Unlock()
	//return ch, err
}

func (t *SignalFxDatasource) buildTargets(query *backend.DataQuery) (*Target, error) {
	target := Target{}
	if err := json.Unmarshal([]byte(query.JSON), &target); err != nil {
		return nil, err
	}
	target.MaxDelay = 0
	target.MinResolution = 0

	var intervalMs = query.Interval.Milliseconds()
	if intervalMs < target.MinResolution {
		intervalMs = target.MinResolution
	}
	target.Interval = time.Duration(intervalMs) * time.Millisecond
	target.StartTime = query.TimeRange.From
	target.StopTime = query.TimeRange.To
	return &target, nil
}

func (t *SignalFxDatasource) cleanup(ticker *time.Ticker) {
	for time := range ticker.C {
		t.cleanupInactiveJobHandlers(time)
	}
}

func (t *SignalFxDatasource) cleanupInactiveJobHandlers(time time.Time) {
	t.handlerMutex.Lock()
	active := make([]SignalFxJob, 0)
	for _, h := range t.handlers {
		if h.isActive(time) {
			active = append(active, h)
		} else {
			log.DefaultLogger.Debug("Stopping inactive job", "program", h.Program())
			h.stop()
		}
	}
	t.handlers = active
	t.handlerMutex.Unlock()
}
