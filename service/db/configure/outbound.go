package configure

type ObservatoryType string

func (t ObservatoryType) String() string {
	return string(t)
}

const (
	LeastPing ObservatoryType = "leastping"
	// LeastPingDownload is a leastping balancer whose probe downloads the
	// body's payload instead of timing only the response headers: a
	// throttled or flaky outbound measures slow and loses the balancer,
	// which a HEAD probe cannot see.
	LeastPingDownload ObservatoryType = "leastpingdl"
)

// StrategyAndMethod maps the observatory type onto the balancer strategy the
// core knows and the probe's HTTP method. The core has no download-probing
// strategy of its own; the variant is expressed through the observer's
// pingConfig.
func (t ObservatoryType) StrategyAndMethod() (strategy, method string) {
	if t == LeastPingDownload {
		return string(LeastPing), "GET"
	}
	return t.String(), "HEAD"
}

type OutboundSetting struct {
	ProbeURL      string          `json:"probeURL"`
	ProbeInterval string          `json:"probeInterval"`
	// ProbeSampling is the number of probes per sampling window; zero keeps
	// the core's default (10).
	ProbeSampling int `json:"probeSampling,omitempty"`
	// ProbeTimeout is a Go duration for one probe; empty keeps the core's
	// default (5s).
	ProbeTimeout string          `json:"probeTimeout,omitempty"`
	Type         ObservatoryType `json:"type"`
	// Selected is the share link of the member the group routes through
	// alone; empty means every member, balanced by the observatory. A link
	// that matches no member is ignored, so a removed or renamed node
	// falls back to balancing instead of breaking the group.
	Selected string `json:"selected,omitempty"`
}

// DefaultOutboundSetting returns an OutboundSetting with default values.
func DefaultOutboundSetting() OutboundSetting {
	return OutboundSetting{
		ProbeURL:      DefaultProbeURL,
		ProbeInterval: DefaultProbeInterval,
		Type:          ObservatoryType(DefaultOutboundType),
	}
}
