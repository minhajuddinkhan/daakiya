package daakiya

import "time"

//Configuration Configuration
type Configuration struct {
	//ClientPingTime the time after which every new client is pinged to
	//check if the socket connection is still healthy.
	ClientPingTime time.Duration
}

//NewConfig creates a new daakiya configuration
func NewConfig() Configuration {
	return Configuration{
		ClientPingTime: 5 * time.Second,
	}
}
