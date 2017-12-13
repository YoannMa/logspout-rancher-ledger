package logspoutRancher

import (
	"github.com/fsouza/go-dockerclient"
	"github.com/rancherio/go-rancher/v2"
	"log"
	"os"
)

// Setting package global Rancher API setting
var cattleUrl = os.Getenv("CATTLE_URL")
var cattleAccessKkey = os.Getenv("CATTLE_ACCESS_KEY")
var cattleSecretKey = os.Getenv("CATTLE_SECRET_KEY")

var rancher *client.RancherClient
var cCache map[string]*RancherInfo

func init() {
	rancher = initRancherClient()
	cCache = make(map[string]*RancherInfo)
}

func initRancherClient() *client.RancherClient {

	config := &client.ClientOpts{
		Url:       cattleUrl,
		AccessKey: cattleAccessKkey,
		SecretKey: cattleSecretKey,
	}

	r, err := client.NewRancherClient(config)

	if err != nil {
		log.Fatalf("Unable to establish rancher api connection: %s", err)
	}

	return r
}

// Uses the passed docker id to find the rancher Id
func GetRancherId(cID string) *client.Container {

	// This adds a filter to search for the specific container we just received an event from
	filters := map[string]interface{}{"externalId": cID}

	listOpts := &client.ListOpts{Filters: filters}

	container, err := rancher.Container.List(listOpts)

	if err != nil {
		log.Print(err)
	}

	// There should only ever be 1 container in the list thanks to our filter
	for _, data := range container.Data {
		if data.ExternalId == cID {
			return &data
		}
	}
	return nil
}

// Add the RancherInfo to the cache
func Cache(con *RancherInfo) {
	cCache[con.Container.DockerID] = con
}

// Check if the container data already exists in the cached map
func ExistsInCache(containerID string) bool {
	for k := range cCache {
		if k == containerID {
			return true
		}
	}

	return false
}

// Get the container data from the map
func GetFromCache(cID string) *RancherInfo {
	return cCache[cID]
}

func DeleteFromCache(cId string) bool {
	delete(cCache, cId)

	return ExistsInCache(cId)
}

// Get the rancher meteadata from the api/cahce
func GetRancherInfo(c *docker.Container) *RancherInfo {
	var rcontainer *client.Container

	// Check if we have added this container to cache before
	if !ExistsInCache(c.ID) {

		// Pull rancher data from the API instead of the docker.sock
		// First we use the docker id to pull the rancher container data
		rcontainer = GetRancherId(c.ID)

		// Since its not in cache go get it
		// Get container data, service data, and stack data if available
		if rcontainer == nil {
			del := DeleteFromCache(c.ID)

			if del {
				log.Printf("Removed container ID %s from cache", c.ID)
			} else {
				return nil
			}

			log.Print("Could not find rancher metadata in the API")
			return nil
		}

		// Fill out container data
		container := &RancherContainer{
			Name:     rcontainer.Name,
			IP:       rcontainer.Ip,
			ID:       rcontainer.Id,
			HostID:   rcontainer.HostId,
			DockerID: c.ID,
			Labels:   rcontainer.Labels,
		}

		rancherInfo := &RancherInfo{
			Container: container,
		}

		Cache(rancherInfo)

		return rancherInfo
	}

	return GetFromCache(c.ID)
}

// Container Docker info for event data
type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

// Rancher data for evetn data
type RancherInfo struct {
	Container *RancherContainer `json:"container,omitempty"`
	//Stack     *RancherStack     `json:"stack,omitempty"`
}

// Rancher container data for event
type RancherContainer struct {
	Name           string                 `json:"name"`
	IP             string                 `json:"ip,omitempty"`
	ID             string                 `json:"rancherId,omitempty"`
	HostID         string                 `json:"hostId,omitempty"`
	DockerID       string                 `json:"dockerId,omitempty"`
	Labels         map[string]interface{} `json:"labels,omitempty"`
}

// Rancher stack inf for event
//type RancherStack struct {
//	Service      string          `json:"service,omitempty"`
//	ServiceId    string          `json:"ServiceId,omitempty"`
//	StackId      string          `json:"StackId,omitempty"`
//	StackName    string          `json:"stackName,omitempty"`
//	StackState   string          `json:"stackState,omitempty"`
//	DebugStack   *client.Stack   `json:"debugStack,omitempty"`
//	DebugService *client.Service `json:"debugService,omitempty"`
//}
