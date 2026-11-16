package v2ray

import (
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"testing"
	"time"

	statscommand "github.com/v2fly/v2ray-core/v5/app/stats/command"
	"github.com/v2rayA/v2rayA/db/configure"
	"google.golang.org/grpc"
)

func TestAPITemplateEnablesOutboundStats(t *testing.T) {
	tmpl := baseTemplate(t, configure.NewSetting())
	if _, err := tmpl.SetAPI(nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tmpl.Close() })
	var config struct {
		Stats  json.RawMessage `json:"stats"`
		Policy struct {
			System struct {
				Uplink   bool `json:"statsOutboundUplink"`
				Downlink bool `json:"statsOutboundDownlink"`
			} `json:"system"`
		} `json:"policy"`
		API struct {
			Services []string `json:"services"`
		} `json:"api"`
	}
	if err := json.Unmarshal(tmpl.ToConfigBytes(), &config); err != nil {
		t.Fatal(err)
	}
	if string(config.Stats) != "{}" {
		t.Errorf("stats = %s, want {}", config.Stats)
	}
	if !config.Policy.System.Uplink || !config.Policy.System.Downlink {
		t.Errorf("outbound stats policy = %+v, want both directions enabled", config.Policy.System)
	}
	if !contains(config.API.Services, "StatsService") {
		t.Errorf("API services = %v, missing StatsService", config.API.Services)
	}
}

func TestTrafficCounterRates(t *testing.T) {
	var counter trafficCounter
	now := time.Unix(100, 0)
	stats := []*statscommand.Stat{
		{Name: "outbound>>>proxy-a>>>traffic>>>uplink", Value: 400},
		{Name: "outbound>>>proxy-b>>>traffic>>>uplink", Value: 600},
		{Name: "outbound>>>proxy-a>>>traffic>>>downlink", Value: 800},
		{Name: "outbound>>>proxy-b>>>traffic>>>downlink", Value: 1200},
		{Name: "inbound>>>proxy-a>>>traffic>>>uplink", Value: 9000},
	}
	for _, tag := range []string{"direct", "block", "dns-out", "api-out"} {
		stats = append(stats,
			&statscommand.Stat{Name: "outbound>>>" + tag + ">>>traffic>>>uplink", Value: 9000},
			&statscommand.Stat{Name: "outbound>>>" + tag + ">>>traffic>>>downlink", Value: 9000},
		)
	}
	if got := counter.sample(stats, now); got.Up != 0 || got.Down != 0 || got.UpTotal != 1000 || got.DownTotal != 2000 {
		t.Fatalf("first sample = %+v, want zero rates and totals 1000/2000", got)
	}
	stats[0].Value += 100
	stats[1].Value += 200
	stats[2].Value += 200
	stats[3].Value += 400
	if got := counter.sample(stats, now.Add(2500*time.Millisecond)); got.Up != 120 || got.Down != 240 || got.UpTotal != 1300 || got.DownTotal != 2600 {
		t.Fatalf("second sample = %+v, want rates 120/240 and totals 1300/2600", got)
	}
}

func TestTrafficCounterReset(t *testing.T) {
	var counter trafficCounter
	now := time.Unix(100, 0)
	stats := []*statscommand.Stat{
		{Name: "outbound>>>proxy>>>traffic>>>uplink", Value: 1000},
		{Name: "outbound>>>proxy>>>traffic>>>downlink", Value: 2000},
	}
	counter.sample(stats, now)
	stats[0].Value, stats[1].Value = 10, 20
	if got := counter.sample(stats, now.Add(time.Second)); got.Up != 0 || got.Down != 0 || got.UpTotal != 10 || got.DownTotal != 20 {
		t.Fatalf("reset sample = %+v, want zero rates and totals 10/20", got)
	}
	stats[0].Value, stats[1].Value = 30, 60
	if got := counter.sample(stats, now.Add(2*time.Second)); got.Up != 20 || got.Down != 40 || got.UpTotal != 30 || got.DownTotal != 60 {
		t.Fatalf("sample after reset = %+v, want rates 20/40 and totals 30/60", got)
	}
}

func TestTrafficProducerPublishesStats(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ interface{}, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if method != "/xray.app.stats.command.StatsService/QueryStats" {
			return fmt.Errorf("unexpected method %q", method)
		}
		var request statscommand.QueryStatsRequest
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		if request.Pattern != "outbound>>>" || request.Reset_ {
			return fmt.Errorf("unexpected query: %v", &request)
		}
		return stream.SendMsg(&statscommand.QueryStatsResponse{Stat: []*statscommand.Stat{
			{Name: "outbound>>>proxy>>>traffic>>>uplink", Value: 123},
			{Name: "outbound>>>proxy>>>traffic>>>downlink", Value: 456},
		}})
	}))
	t.Cleanup(server.Stop)
	go server.Serve(listener)
	box := ApiFeed.SubscribeMessage("traffic")
	if box == nil {
		t.Fatal("traffic product is not registered")
	}
	t.Cleanup(box.Cancel)
	t.Cleanup(TrafficProducer(listener.Addr().(*net.TCPAddr).Port))
	select {
	case message := <-box.Messages:
		if message.Product != "traffic" {
			t.Fatalf("product = %q, want traffic", message.Product)
		}
		body, err := json.Marshal(message.Body)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]float64
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if !maps.Equal(got, map[string]float64{"up": 0, "down": 0, "upTotal": 123, "downTotal": 456}) {
			t.Fatalf("traffic body = %s", body)
		}
	case <-time.After(5 * ApiFeedInterval):
		t.Fatal("no traffic frame received")
	}
}

func TestActiveOutboundsMapsTagsToConnectedServers(t *testing.T) {
	refs := configure.NodeRefs{}
	refs.Add(configure.NodeRef{TYPE: configure.ServerType, ID: 1, Outbound: "proxy"})
	refs.Add(configure.NodeRef{TYPE: configure.ServerType, ID: 2, Outbound: "proxy"})
	if err := configure.OverwriteConnects(&refs); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = configure.ClearConnects("proxy") })
	p := &Process{tag2WhichIndex: map[string]int{"『A』": 0, "『B』": 1}}
	active := activeOutbounds(p, map[string]tagRate{
		"『A』": {Up: 100, Down: 200},
		"『B』": {Up: 10, Down: 20},
		"『X』": {Up: 5, Down: 5}, // not in the template: dropped
	})
	if len(active) != 2 {
		t.Fatalf("active = %+v, want the two mapped members", active)
	}
	// busiest first, so the reader can take the head
	if active[0].Which.ID != 1 || active[0].Up != 100 || active[0].Down != 200 {
		t.Fatalf("active[0] = %+v, want member 1 with rates 100/200", active[0])
	}
	if active[1].Which.ID != 2 || active[1].Up != 10 || active[1].Down != 20 {
		t.Fatalf("active[1] = %+v, want member 2 with rates 10/20", active[1])
	}
}
