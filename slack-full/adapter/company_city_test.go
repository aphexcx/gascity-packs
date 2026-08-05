package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// City-qualified bindings: one switchboard delivers wakes to sessions in
// other gc cities (the live org runs one team per city, each with its own
// supervisor API).

func cityTestBindingsJSON(t *testing.T, city string) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"bindings": []map[string]string{
			{"room": "orchestrator-team", "agent": "ollie", "session": "teams__pm", "city": city},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestParseCompanyBindingsCityField(t *testing.T) {
	dir, err := ParseCompanyDirectory(marshalDirectory(t, baseDirectoryFile()))
	if err != nil {
		t.Fatal(err)
	}
	b, warnings, err := ParseCompanyBindings(cityTestBindingsJSON(t, "platform"), dir)
	if err != nil || len(warnings) != 0 {
		t.Fatalf("parse: err=%v warnings=%v", err, warnings)
	}
	bd, ok := b.BindingFor("orchestrator-team", "ollie")
	if !ok || bd.City != "platform" || bd.Session != "teams__pm" {
		t.Fatalf("BindingFor = %+v, %v; want city=platform session=teams__pm", bd, ok)
	}
	// SessionFor stays city-agnostic for existence checks.
	if s, ok := b.SessionFor("orchestrator-team", "ollie"); !ok || s != "teams__pm" {
		t.Fatalf("SessionFor = %q, %v", s, ok)
	}
}

func TestParseCompanyBindingsCityValidation(t *testing.T) {
	dir, err := ParseCompanyDirectory(marshalDirectory(t, baseDirectoryFile()))
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"a/b", "a?b", "a#b", "a%b", "a b", "a\tb"} {
		if _, _, err := ParseCompanyBindings(cityTestBindingsJSON(t, bad), dir); err == nil {
			t.Errorf("city %q accepted; want URL-significant rejection", bad)
		}
	}
}

// TestDeliverCrossCityUsesMappedAPIBase proves a city-qualified target is
// POSTed to the TARGET city's supervisor base with the city in the URL
// path, and that an unmapped city fails definitively (no retry loop).
func TestDeliverCrossCityUsesMappedAPIBase(t *testing.T) {
	ownHits, otherHits := 0, 0
	own := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownHits++
		w.WriteHeader(http.StatusOK)
	}))
	defer own.Close()
	var otherPath string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherHits++
		otherPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()

	g := &companyGateway{
		cfg: config{
			cityName:        "gas-city-inc",
			gcAPIBase:       own.URL,
			companyCityAPIs: map[string]string{"platform": other.URL},
		},
		deliverClient: &http.Client{Timeout: 5 * time.Second},
	}

	// Cross-city target → the mapped base, city in the path.
	result := g.postCompanyMessage(TargetDelivery{
		Session: "teams__pm", City: "platform",
		IdempotencyKey: "ingress:x:target:teams__pm",
	}, "body")
	if result.disposition != postDelivered {
		t.Fatalf("cross-city delivery failed: disp=%v detail=%s", result.disposition, result.detail)
	}
	if otherHits != 1 || ownHits != 0 {
		t.Fatalf("hits own=%d other=%d; want 0/1", ownHits, otherHits)
	}
	if want := "/v0/city/platform/session/teams__pm/messages"; otherPath != want {
		t.Fatalf("path = %q, want %q", otherPath, want)
	}

	// Own-city target (empty City) → the adapter's own base.
	result = g.postCompanyMessage(TargetDelivery{
		Session: "s-local", IdempotencyKey: "ingress:x:target:s-local",
	}, "body")
	if result.disposition != postDelivered || ownHits != 1 {
		t.Fatalf("own-city delivery: disp=%v ownHits=%d detail=%s", result.disposition, ownHits, result.detail)
	}

	// Unmapped city → definitive failure, no HTTP call.
	result = g.postCompanyMessage(TargetDelivery{
		Session: "s-x", City: "unmapped-city",
		IdempotencyKey: "ingress:x:target:s-x",
	}, "body")
	if result.disposition != postDefinitive {
		t.Fatalf("unmapped city: disp=%v; want postDefinitive", result.disposition)
	}
	if result.detail == "" {
		t.Fatal("unmapped city: empty detail")
	}
	if ownHits != 1 || otherHits != 1 {
		t.Fatalf("unmapped city made an HTTP call (own=%d other=%d)", ownHits, otherHits)
	}
}

// TestEnsureTargetsRecordsBindingCity proves the frozen target carries the
// binding's city so redrives and pointers stay city-stable.
func TestEnsureTargetsRecordsBindingCity(t *testing.T) {
	dir, err := ParseCompanyDirectory(marshalDirectory(t, baseDirectoryFile()))
	if err != nil {
		t.Fatal(err)
	}
	bindings, _, err := ParseCompanyBindings(cityTestBindingsJSON(t, "platform"), dir)
	if err != nil {
		t.Fatal(err)
	}
	room, _ := dir.RoomByChannel("T0AAAAAAA", "C0AAAAAAA")
	agent, _ := dir.AgentByName("ollie")
	g := &companyGateway{cfg: config{cityName: "gas-city-inc"}}
	r := &IngressReceipt{ID: "in-test"}
	g.ensureTargets(r, room, []frozenWake{{Agent: *agent, Kind: "ambient"}}, bindings, time.Now())
	found := false
	for _, td := range r.Targets {
		if td.Session == "teams__pm" {
			found = true
			if td.City != "platform" {
				t.Fatalf("target city = %q, want platform", td.City)
			}
		}
	}
	if !found {
		t.Fatalf("no bound target recorded: %+v", r.Targets)
	}
}
