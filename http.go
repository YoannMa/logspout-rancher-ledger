package logspoutRancher

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gliderlabs/logspout/router"
	"github.com/fsouza/go-dockerclient"
)

func debug(v ...interface{}) {
	if os.Getenv("DEBUG") != "" {
		log.Println(v...)
	}
}

func die(v ...interface{}) {
	panic(fmt.Sprintln(v...))
}

func getStringParameter(
	options map[string]string, parameterName string, dfault string) string {

	if value, ok := options[parameterName]; ok {
		return value
	} else {
		return dfault
	}
}

func getIntParameter(
	options map[string]string, parameterName string, dfault int) int {

	if value, ok := options[parameterName]; ok {
		valueInt, err := strconv.Atoi(value)
		if err != nil {
			debug("http: invalid value for parameter:", parameterName, value)
			return dfault
		} else {
			return valueInt
		}
	} else {
		return dfault
	}
}

func getDurationParameter(
	options map[string]string, parameterName string,
	dfault time.Duration) time.Duration {

	if value, ok := options[parameterName]; ok {
		valueDuration, err := time.ParseDuration(value)
		if err != nil {
			debug("http: invalid value for parameter:", parameterName, value)
			return dfault
		} else {
			return valueDuration
		}
	} else {
		return dfault
	}
}

func dial(netw, addr string) (net.Conn, error) {
	dial, err := net.Dial(netw, addr)
	if err != nil {
		debug("http: new dial", dial, err, netw, addr)
	} else {
		debug("http: new dial", dial, netw, addr)
	}
	return dial, err
}

// HTTPAdapter is an adapter that POSTs logs to an HTTP endpoint
type HTTPAdapter struct {
	route  *router.Route
	url    string
	client *http.Client
	buffer []*map[string]interface{}
	timer             *time.Timer
	capacity          int
	timeout           time.Duration
	totalMessageCount int
	bufferMutex       sync.Mutex
	useGzip           bool
	crash             bool
	logstashFields    map[string]map[string]string
}

// NewHTTPAdapter creates an HTTPAdapter
func NewHTTPAdapter(route *router.Route) (router.LogAdapter, error) {

	// Figure out the URI and create the HTTP client
	defaultPath := ""
	path := getStringParameter(route.Options, "http.path", defaultPath)
	endpointUrl := fmt.Sprintf("%s://%s%s", route.Adapter, route.Address, path)
	debug("http: url:", endpointUrl)
	transport := &http.Transport{}
	transport.Dial = dial

	// Figure out if we need a proxy
	defaultProxyUrl := ""
	proxyUrlString := getStringParameter(route.Options, "http.proxy", defaultProxyUrl)
	if proxyUrlString != "" {
		proxyUrl, err := url.Parse(proxyUrlString)
		if err != nil {
			die("", "http: cannot parse proxy url:", err, proxyUrlString)
		}
		transport.Proxy = http.ProxyURL(proxyUrl)
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		debug("http: proxy url:", proxyUrl)
	}

	// Create the client
	client := &http.Client{Transport: transport}

	// Determine the buffer capacity
	defaultCapacity := 100
	capacity := getIntParameter(
		route.Options, "http.buffer.capacity", defaultCapacity)
	if capacity < 1 || capacity > 10000 {
		debug("http: non-sensical value for parameter: http.buffer.capacity",
			capacity, "using default:", defaultCapacity)
		capacity = defaultCapacity
	}
	buffer := make([]*map[string]interface{}, 0, capacity)

	// Determine the buffer timeout
	defaultTimeout, _ := time.ParseDuration("1000ms")
	timeout := getDurationParameter(
		route.Options, "http.buffer.timeout", defaultTimeout)
	timeoutSeconds := timeout.Seconds()
	if timeoutSeconds < .1 || timeoutSeconds > 600 {
		debug("http: non-sensical value for parameter: http.buffer.timeout",
			timeout, "using default:", defaultTimeout)
		timeout = defaultTimeout
	}
	timer := time.NewTimer(timeout)

	// Figure out whether we should use GZIP compression
	useGzip := false
	useGZipString := getStringParameter(route.Options, "http.gzip", "false")
	if useGZipString == "true" {
		useGzip = true
		debug("http: gzip compression enabled")
	}

	// Should we crash on an error or keep going?
	crash := true
	crashString := getStringParameter(route.Options, "http.crash", "true")
	if crashString == "false" {
		crash = false
		debug("http: don't crash, keep going")
	}

	// Make the HTTP adapter
	return &HTTPAdapter{
		route:          route,
		url:            endpointUrl,
		client:         client,
		buffer:         buffer,
		timer:          timer,
		capacity:       capacity,
		timeout:        timeout,
		useGzip:        useGzip,
		crash:          crash,
		logstashFields: make(map[string]map[string]string),
	}, nil
}

// Flushes the accumulated messages in the buffer
func (a *HTTPAdapter) flushHttp(reason string) {

	// Stop the timer and drain any possible remaining events
	a.timer.Stop()
	select {
	case <-a.timer.C:
	default:
	}

	// Reset the timer when we are done
	defer a.timer.Reset(a.timeout)

	// Return immediately if the buffer is empty
	if len(a.buffer) < 1 {
		return
	}

	// Capture the buffer and make a new one
	a.bufferMutex.Lock()
	buffer := a.buffer
	a.buffer = make([]*map[string]interface{}, 0, a.capacity)
	a.bufferMutex.Unlock()

	// Create JSON representation of all messages
	payload, err := json.Marshal(buffer)
	if err != nil {
		debug("flushHttp - Error encoding JSON: ", err)
		return
	}

	go func() {
		// Create the request and send it on its way
		request := createRequest(a.url, a.useGzip, string(payload))
		start := time.Now()
		response, err := a.client.Do(request)
		if err != nil {
			debug("http - error on client.Do:", err, a.url)
			// TODO @raychaser - now what?
			if a.crash {
				die("http - error on client.Do:", err, a.url)
			} else {
				debug("http: error on client.Do:", err)
			}
		}
		if response.StatusCode != 200 {
			debug("http: response not 200 but", response.StatusCode)
			// TODO @raychaser - now what?
			if a.crash {
				die("http: response not 200 but", response.StatusCode)
			}
		}

		// Make sure the entire response body is read so the HTTP
		// connection can be reused
		io.Copy(ioutil.Discard, response.Body)
		response.Body.Close()

		// Bookkeeping, logging
		timeAll := time.Since(start)
		a.totalMessageCount += len(buffer)
		debug("http: flushed:", reason, "messages:", len(buffer),
			"in:", timeAll, "total:", a.totalMessageCount)
	}()
}

// Create the request based on whether GZIP compression is to be used
func createRequest(url string, useGzip bool, payload string) *http.Request {
	var request *http.Request
	if useGzip {
		gzipBuffer := new(bytes.Buffer)
		gzipWriter := gzip.NewWriter(gzipBuffer)
		_, err := gzipWriter.Write([]byte(payload))
		if err != nil {
			// TODO @raychaser - now what?
			die("http: unable to write to GZIP writer:", err)
		}
		err = gzipWriter.Close()
		if err != nil {
			// TODO @raychaser - now what?
			die("http: unable to close GZIP writer:", err)
		}
		request, err = http.NewRequest("POST", url, gzipBuffer)
		if err != nil {
			debug("http: error on http.NewRequest:", err, url)
			// TODO @raychaser - now what?
			die("", "http: error on http.NewRequest:", err, url)
		}
		request.Header.Set("Content-Encoding", "gzip")
	} else {
		var err error
		request, err = http.NewRequest("POST", url, strings.NewReader(payload))
		if err != nil {
			debug("http: error on http.NewRequest:", err, url)
			// TODO @raychaser - now what?
			die("", "http: error on http.NewRequest:", err, url)
		}
	}
	return request
}

// Parse the logstash fields env variables
func GetLogstashFields(c *docker.Container, a *HTTPAdapter) map[string]string {
	if fields, ok := a.logstashFields[c.ID]; ok {
		return fields
	}

	fieldsStr := os.Getenv("LOGSTASH_FIELDS")
	fields := map[string]string{}

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_FIELDS=") {
			fieldsStr = strings.TrimPrefix(e, "LOGSTASH_FIELDS=")
		}
	}

	if len(fieldsStr) > 0 {
		for _, f := range strings.Split(fieldsStr, ",") {
			sp := strings.Split(f, "=")
			k, v := sp[0], sp[1]
			fields[k] = v
		}
	}

	a.logstashFields[c.ID] = fields

	return fields
}
