package v2ray

import (
	"testing"

	"github.com/v2rayA/v2rayA/db/configure"
	"github.com/v2rayA/v2rayA/kernel/coreObj"
	"github.com/v2rayA/v2rayA/kernel/serverObj"
)

func observatoryServerData(setting configure.OutboundSetting) *ServerData {
	return &ServerData{
		OutboundName2Setting: map[string]configure.OutboundSetting{"proxy": setting},
		OutboundName2ServerObjs: map[string][]serverObj.ServerObj{
			"proxy": {&serverObj.SOCKS{Name: "a", Server: "192.0.2.1", Port: 1080, Protocol: "socks5"}},
		},
	}
}

func groupTemplate(t *testing.T, data *ServerData) *Template {
	t.Helper()
	tmpl := &Template{
		Outbounds: []coreObj.OutboundObject{{Tag: "proxy", Balancers: []string{"proxy"}}},
	}
	if _, err := tmpl.SetAPI(data); err != nil {
		t.Fatal(err)
	}
	return tmpl
}

// The download variant of the auto balancing is a leastping balancer whose
// observer downloads the probe body, so a throttled or flaky member
// measures slow; the sampling and timeout knobs reach the pingConfig
// unchanged.
func TestLeastPingDownloadTemplate(t *testing.T) {
	tmpl := groupTemplate(t, observatoryServerData(configure.OutboundSetting{
		Type:          configure.LeastPingDownload,
		ProbeURL:      "https://speed.cloudflare.com/__down?bytes=40000",
		ProbeInterval: "60s",
		ProbeSampling: 3,
		ProbeTimeout:  "8s",
	}))

	if len(tmpl.Routing.Balancers) != 1 || tmpl.Routing.Balancers[0].Strategy.Type != "leastping" {
		t.Fatalf("balancers = %+v, the core knows no download strategy", tmpl.Routing.Balancers)
	}
	if tmpl.MultiObservatory == nil || len(tmpl.MultiObservatory.Observers) != 1 {
		t.Fatalf("multiObservatory = %+v", tmpl.MultiObservatory)
	}
	pc := tmpl.MultiObservatory.Observers[0].Settings.PingConfig
	if pc == nil {
		t.Fatal("no pingConfig")
	}
	if pc.HTTPMethod != "GET" || pc.Destination != "https://speed.cloudflare.com/__down?bytes=40000" ||
		pc.SamplingCount != 3 || pc.Timeout != "8s" || pc.Interval != "1m0s" {
		t.Fatalf("pingConfig = %+v", pc)
	}
}

// The plain auto mode keeps timing response headers and leaves the knobs at
// the core's defaults.
func TestLeastPingTemplateKeepsHeadProbes(t *testing.T) {
	tmpl := groupTemplate(t, observatoryServerData(configure.OutboundSetting{
		Type:          configure.LeastPing,
		ProbeURL:      "https://www.gstatic.com/generate_204",
		ProbeInterval: "60s",
	}))
	pc := tmpl.MultiObservatory.Observers[0].Settings.PingConfig
	if pc == nil || pc.HTTPMethod != "HEAD" || pc.SamplingCount != 0 || pc.Timeout != "" {
		t.Fatalf("pingConfig = %+v", pc)
	}
}

// A malformed probe timeout must not reach the core, where it would fail
// the whole configuration; it is dropped with a warning instead.
func TestInvalidProbeTimeoutIsDropped(t *testing.T) {
	tmpl := groupTemplate(t, observatoryServerData(configure.OutboundSetting{
		Type:          configure.LeastPingDownload,
		ProbeURL:      "https://speed.cloudflare.com/__down?bytes=40000",
		ProbeInterval: "60s",
		ProbeTimeout:  "not-a-duration",
		ProbeSampling: -1,
	}))
	pc := tmpl.MultiObservatory.Observers[0].Settings.PingConfig
	if pc == nil || pc.Timeout != "" || pc.SamplingCount != 0 {
		t.Fatalf("pingConfig = %+v", pc)
	}
}
