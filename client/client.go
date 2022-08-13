package veap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/mdzio/go-lib/any"
	"github.com/mdzio/go-veap"
	"github.com/mdzio/go-veap/encoding"

	"github.com/mdzio/go-logging"
)

const (
	// default max. size of a valid response: 1 MB
	defaultResponseSizeLimit = 1 * 1024 * 1024
)

// Client forwards service calls to a remote VEAP server. It implements veap.Service.
type Client struct {
	// URL of the VEAP server, without a trailing slash.
	URL string

	// ResponseSizeLimit is the maximum size of a valid response. If not set, the
	// limit is 1 MB.
	ResponseSizeLimit int

	// Use a specific HTTP client. If not set, the default client is used.
	Client *http.Client

	// Use a specific Logger. If not set, logging.Get("veap-client") is used.
	Log logging.Logger
}

// Init initializes the Client. This function must be called before use.
func (c *Client) Init() {
	if c.ResponseSizeLimit == 0 {
		c.ResponseSizeLimit = defaultResponseSizeLimit
	}
	if c.Client == nil {
		c.Client = http.DefaultClient
	}
	if c.Log == nil {
		c.Log = logging.Get("veap-client")
	}
}

// ReadPV reads the process value of a data point. The path must not end with /~pv.
// VEAP-Protocol: HTTP-GET on PV (.../~pv)
func (c *Client) ReadPV(path string) (veap.PV, veap.Error) {
	// do request
	url := c.URL + path + "/" + veap.PVMarker
	c.Log.Debugf("Sending HTTP-GET request to %s", url)
	resp, err := c.Client.Get(url)
	if err != nil {
		return veap.PV{}, veap.NewErrorf(veap.StatusClientError, "HTTP-GET on %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	respBytes, err := c.readLimited(resp.Body)
	if err != nil {
		return veap.PV{}, veap.NewError(veap.StatusClientError, err)
	}
	if resp.StatusCode != veap.StatusOK {
		return veap.PV{}, veap.NewErrorf(resp.StatusCode, "Received HTTP status: %d (%s)",
			resp.StatusCode, string(respBytes))
	}

	// log response
	if c.Log.TraceEnabled() {
		c.Log.Tracef("Response body: %s", string(respBytes))
	}

	// unmarshal JSON
	pv, err := encoding.BytesToPV(respBytes, false /* no fuzzy parsing */)
	if err != nil {
		return veap.PV{}, veap.NewErrorf(veap.StatusClientError, "Conversion of JSON to PV failed: %v", err)
	}
	return pv, nil
}

// WritePV sets the process value of a data point. VEAP-Protocol: HTTP-PUT
// on PV (.../~pv)
func (c *Client) WritePV(path string, pv veap.PV) veap.Error {
	// convert PV to JSON
	url := c.URL + path + "/" + veap.PVMarker
	c.Log.Debugf("Sending HTTP-PUT request to %s", url)
	reqBytes, err := json.Marshal(encoding.PVToWire(pv))
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "Conversion of PV to JSON failed: %v", err)
	}

	// log request
	if c.Log.TraceEnabled() {
		c.Log.Tracef("Request body: %s", string(reqBytes))
	}

	// do request
	buf := bytes.NewBuffer(reqBytes)
	req, err := http.NewRequest(http.MethodPut, url, buf)
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "Creating HTTP-PUT request failed: %v", err)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "HTTP-PUT request failed: %v", err)
	}
	defer resp.Body.Close()

	// check result
	if resp.StatusCode != veap.StatusOK {
		respBytes, _ := c.readLimited(resp.Body)
		return veap.NewErrorf(resp.StatusCode, "Received HTTP status: %d (%s)",
			resp.StatusCode, string(respBytes))
	}
	return nil
}

// ReadHistory retrieves the history of a data point. The times of the
// returned entries must be in ascending order. VEAP-Protocol: HTTP-GET on
// history (.../~hist)
func (c *Client) ReadHistory(path string, begin time.Time, end time.Time, limit int64) ([]veap.PV, veap.Error) {
	// move timestamps to next millisecond
	begin = begin.Add(999999 * time.Nanosecond).Truncate(time.Millisecond)
	end = end.Add(999999 * time.Nanosecond).Truncate(time.Millisecond)

	// build URL
	beginParam := strconv.FormatInt(begin.UnixNano()/1000000, 10)
	endParam := strconv.FormatInt(end.UnixNano()/1000000, 10)
	limitParam := strconv.FormatInt(limit, 10)
	url := c.URL + path + "/" + veap.HistMarker + "?begin=" + beginParam + "&end=" + endParam + "&limit=" + limitParam
	c.Log.Debugf("Sending HTTP-GET request to %s", url)

	// do request
	resp, err := c.Client.Get(url)
	if err != nil {
		return nil, veap.NewErrorf(veap.StatusClientError, "HTTP-GET on %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	respBytes, err := c.readLimited(resp.Body)
	if err != nil {
		return nil, veap.NewError(veap.StatusClientError, err)
	}
	if resp.StatusCode != veap.StatusOK {
		return nil, veap.NewErrorf(resp.StatusCode, "Received HTTP status: %d (%s)",
			resp.StatusCode, string(respBytes))
	}

	// log response
	if c.Log.TraceEnabled() {
		c.Log.Tracef("Response body: %s", string(respBytes))
	}

	// convert JSON to history
	var w encoding.WireHist
	err = json.Unmarshal(respBytes, &w)
	if err != nil {
		return nil, veap.NewErrorf(veap.StatusClientError, "Conversion of JSON to history failed: %v", err)
	}
	hist, err := encoding.WireToHist(w)
	if err != nil {
		return nil, veap.NewErrorf(veap.StatusClientError, "%v", err)
	}
	return hist, nil
}

// WriteHistory replaces the history of a data point. The replaced time
// range goes from the minimum timestamp to the maximum timestamp.
// VEAP-Protocol: HTTP-PUT on history (.../~hist)
func (c *Client) WriteHistory(path string, timeSeries []veap.PV) veap.Error {
	// convert history to JSON
	url := c.URL + path + "/" + veap.HistMarker
	c.Log.Debugf("Sending HTTP-PUT request to %s", url)
	reqBytes, err := json.Marshal(encoding.HistToWire(timeSeries))
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "Conversion of history to JSON failed: %v", err)
	}

	// log request
	if c.Log.TraceEnabled() {
		c.Log.Tracef("Request body: %s", string(reqBytes))
	}

	// do request
	buf := bytes.NewBuffer(reqBytes)
	req, err := http.NewRequest(http.MethodPut, url, buf)
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "Creating HTTP-PUT request failed: %v", err)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "HTTP-PUT request failed: %v", err)
	}
	defer resp.Body.Close()

	// check result
	if resp.StatusCode != veap.StatusOK {
		respBytes, _ := c.readLimited(resp.Body)
		return veap.NewErrorf(resp.StatusCode, "Received HTTP status: %d (%s)",
			resp.StatusCode, string(respBytes))
	}
	return nil
}

// ReadProperties returns the attributes and links of a VEAP object.
// Attribute values must be supported by package json. VEAP-Protocol:
// HTTP-GET on object
func (c *Client) ReadProperties(path string) (veap.AttrValues, []veap.Link, veap.Error) {
	// do request
	url := c.URL + path
	c.Log.Debugf("Sending HTTP-GET request to %s", url)
	resp, err := c.Client.Get(url)
	if err != nil {
		return nil, nil, veap.NewErrorf(veap.StatusClientError, "HTTP-GET on %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	respBytes, err := c.readLimited(resp.Body)
	if err != nil {
		return nil, nil, veap.NewError(veap.StatusClientError, err)
	}
	if resp.StatusCode != veap.StatusOK {
		return nil, nil, veap.NewErrorf(resp.StatusCode, "Received HTTP status: %d (%s)",
			resp.StatusCode, string(respBytes))
	}

	// log response
	if c.Log.TraceEnabled() {
		c.Log.Tracef("Response body: %s", string(respBytes))
	}

	// unmarshal JSON
	var attr map[string]interface{}
	err = json.Unmarshal(respBytes, &attr)
	if err != nil {
		return nil, nil, veap.NewErrorf(veap.StatusClientError, "Invalid JSON object: %v", err)
	}

	// extract ~links
	var links []veap.Link
	query := any.Q(attr)
	mqattr := query.Map() // can't fail
	for _, qlink := range mqattr.TryKey(veap.LinksMarker).Slice() {
		mqlink := qlink.Map()
		links = append(links, veap.Link{
			Role:   mqlink.TryKey("rel").String(),
			Target: mqlink.TryKey("href").String(),
			Title:  mqlink.TryKey("title").String(),
		})
	}
	if query.Err() != nil {
		return nil, nil, veap.NewErrorf(veap.StatusClientError, "Invalid ~links property: %v", query.Err())
	}

	// remove ~links to get remaining attributes
	delete(attr, veap.LinksMarker)

	return attr, links, nil
}

// WriteProperties updates properties of an existing VEAP object. If no
// object exists at the specified path, a new object is created. Links are
// intentionally not handled. (A concept is still pending.) Attributes were
// unmarshalled with package json. VEAP-Protocol: HTTP-PUT on object
func (c *Client) WriteProperties(path string, attributes veap.AttrValues) (bool, veap.Error) {
	// convert attributes to JSON
	url := c.URL + path
	c.Log.Debugf("Sending HTTP-PUT request to %s", url)
	reqBytes, err := json.Marshal(attributes)
	if err != nil {
		return false, veap.NewErrorf(veap.StatusBadRequest, "Conversion of attributes to JSON failed: %v", err)
	}

	// log request
	if c.Log.TraceEnabled() {
		c.Log.Tracef("Request body: %s", string(reqBytes))
	}

	// do request
	reqReader := bytes.NewBuffer(reqBytes)
	req, err := http.NewRequest(http.MethodPut, url, reqReader)
	if err != nil {
		return false, veap.NewErrorf(veap.StatusClientError, "Creating HTTP-PUT request failed: %v", err)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return false, veap.NewErrorf(veap.StatusClientError, "HTTP-PUT request failed: %v", err)
	}
	defer resp.Body.Close()

	// check result
	if resp.StatusCode != veap.StatusOK && resp.StatusCode != veap.StatusCreated {
		respBytes, _ := c.readLimited(resp.Body)
		return false, veap.NewErrorf(resp.StatusCode, "Received HTTP status: %d (%s)",
			resp.StatusCode, string(respBytes))
	}
	return resp.StatusCode == veap.StatusCreated, nil
}

// Delete destroys a VEAP object. VEAP-Protocol: HTTP-DELETE on object
func (c *Client) Delete(path string) veap.Error {
	// do request
	url := c.URL + path
	c.Log.Debugf("Sending HTTP-DELETE request to %s", url)
	reqReader := &bytes.Buffer{}
	req, err := http.NewRequest(http.MethodDelete, url, reqReader)
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "Creating HTTP-DELETE request failed: %v", err)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return veap.NewErrorf(veap.StatusClientError, "HTTP-DELETE request failed: %v", err)
	}
	defer resp.Body.Close()

	// check result
	if resp.StatusCode != veap.StatusOK {
		respBytes, _ := c.readLimited(resp.Body)
		return veap.NewErrorf(resp.StatusCode, "Received HTTP status: %d (%s)",
			resp.StatusCode, string(respBytes))
	}
	return nil
}

func (c *Client) readLimited(r io.Reader) ([]byte, error) {
	exceededLimit := c.ResponseSizeLimit + 1
	limitReader := io.LimitReader(r, int64(exceededLimit))
	data, _ := ioutil.ReadAll(limitReader)
	if len(data) == exceededLimit {
		return nil, fmt.Errorf("Response size limit of %d bytes exceeded", c.ResponseSizeLimit)
	}
	return data, nil
}
