package plugin

import (
	"os/exec"
	"strings"

	"github.com/hashicorp/go-plugin"
	"github.com/smallstep/nosql/plugin/shared"
)

// NewPluginClient build new go-plugin client
func NewPluginClient(pluginCmd, pluginName string) (*plugin.Client, shared.DBPlugin, error) {
	cmd := strings.Split(pluginCmd, " ")
	clientConfig := &plugin.ClientConfig{
		HandshakeConfig: shared.Handshake,
		Plugins:         shared.PluginMap,
		Cmd:             exec.Command(cmd[0], cmd[1:]...),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC,
		},
	}
	client := plugin.NewClient(clientConfig)
	c, err := client.Client()
	if err != nil {
		return nil, nil, err
	}
	s, err := c.Dispense(pluginName)
	if err != nil {
		defer c.Close()
		return nil, nil, err
	}

	return client, s.(shared.DBPlugin), nil
}
