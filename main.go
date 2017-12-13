package logspoutRancher

import (
	"encoding/json"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewHTTPAdapter, "http")
	router.AdapterFactories.Register(NewHTTPAdapter, "https")
}

// Stream implements the router.LogAdapter interface
func (a *HTTPAdapter) Stream(logstream chan *router.Message) {
	for {
		select {
		case message := <-logstream:

			dockerInfo := DockerInfo{
				Name:     message.Container.Name,
				ID:       message.Container.ID,
				Image:    message.Container.Config.Image,
				Hostname: message.Container.Config.Hostname,
			}

			fields := GetLogstashFields(message.Container, a)

			rancherInfo := GetRancherInfo(message.Container)

			if rancherInfo == nil {
				continue
			}

			var data map[string]interface{}
			var err error

			// Try to parse JSON-encoded m.Data. If it wasn't JSON, create an empty object
			// and use the original data as the message.
			if err = json.Unmarshal([]byte(message.Data), &data); err != nil {
				data = make(map[string]interface{})
				data["message"] = message.Data
			}

			for k, v := range fields {
				data[k] = v
			}

			data["docker"] = dockerInfo
			data["rancher"] = rancherInfo

			// Append the message to the buffer
			a.bufferMutex.Lock()
			a.buffer = append(a.buffer, &data)
			a.bufferMutex.Unlock()

			// Flush if the buffer is at capacity
			if len(a.buffer) >= cap(a.buffer) {
				a.flushHttp("full")
			}
		case <-a.timer.C:

			// Timeout, flush
			a.flushHttp("timeout")
		}
	}
}

