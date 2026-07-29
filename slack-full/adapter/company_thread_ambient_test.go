package main

import (
	"net/http"
	"strings"
	"testing"
)

func authenticateRileyBot(h *companyHarness) {
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
	}}
}

func rileyThreadPost(ts, root, text string) slackMessageEvent {
	return botEvent("B0RILEY", botRiley, "A0AAAAAA2", ts, root, text, nil)
}

func callTargets(calls []gcDelivery) map[string]bool {
	out := make(map[string]bool)
	for _, call := range calls {
		switch {
		case strings.Contains(call.path, "/session/ollie-main/messages"):
			out["ollie"] = true
		case strings.Contains(call.path, "/session/riley-main/messages"):
			out["riley"] = true
		}
	}
	return out
}

// Once an authenticated company agent has posted in a company-room thread,
// subsequent untagged human replies wake that agent alongside the room's normal
// ambient readers. The participation evidence is receipt-backed, so an adapter
// restart between the agent post and the human follow-up cannot forget it.
func TestCompanyThreadParticipantAmbientSurvivesRestart(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	authenticateRileyBot(h)
	h.openBarrier()

	root := "1700000000.030000"
	botTS := "1700000000.030100"
	if w, handled := h.admitViaHandler(t, rileyThreadPost(botTS, root, "I am looking into it"), 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("Riley post admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()
	botReceipt, _ := h.receipts.Get(ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: botTS})
	if botReceipt == nil || botReceipt.Status != ingressStatusNoDelivery {
		t.Fatalf("Riley receipt status = %v, want no_delivery", statusOf(botReceipt))
	}

	// Rebuild the gateway over the same durable receipt store.
	h.reopen(t, gc.server.URL, 4)
	h.openBarrier()
	followup := humanMessage("1700000000.030200", "any update?")
	followup.ThreadTS = root
	if w, handled := h.admitViaHandler(t, followup, 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("follow-up admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()

	calls := gc.sessionCalls()
	got := callTargets(calls)
	if len(calls) != 2 || !got["ollie"] || !got["riley"] {
		t.Fatalf("follow-up targets = %v (%d calls), want ambient Ollie + thread participant Riley: %+v", got, len(calls), calls)
	}

	r, _ := h.receipts.Get(ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: followup.TS})
	foundThreadAmbient := false
	for _, td := range r.Targets {
		if td.Agent == "riley" && td.Kind == "thread_ambient" {
			foundThreadAmbient = true
		}
	}
	if !foundThreadAmbient {
		t.Fatalf("Riley target was not frozen as %q: %+v", "thread_ambient", r.Targets)
	}
}

// Native company-agent mentions stay exclusive even when another agent is a
// participant in the thread. This preserves the existing direct-addressing
// contract and prevents an explicit mention from fanning out to all readers.
func TestCompanyThreadParticipantsSuppressedByExplicitMention(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	authenticateRileyBot(h)
	h.openBarrier()

	root := "1700000000.031000"
	if _, handled := h.admitViaHandler(t, rileyThreadPost("1700000000.031100", root, "following"), 0); !handled {
		t.Fatal("Riley post not handled")
	}
	h.wait()

	followup := humanMessage("1700000000.031200", "<@"+botOllie+"> take this one")
	followup.ThreadTS = root
	if _, handled := h.admitViaHandler(t, followup, 0); !handled {
		t.Fatal("follow-up not handled")
	}
	h.wait()

	calls := gc.sessionCalls()
	got := callTargets(calls)
	if len(calls) != 1 || !got["ollie"] || got["riley"] {
		t.Fatalf("explicit mention targets = %v (%d calls), want Ollie only: %+v", got, len(calls), calls)
	}
}

// Raw user/app fields are not enrollment authority. A bot must pass the
// bots.info-backed company-author corroboration before its threaded post can
// make the corresponding agent an ambient reader.
func TestCompanyThreadParticipantRequiresAuthenticatedBot(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.gw.authors = fakeAuthorResolver{} // every bot_id is definitively unknown
	h.openBarrier()

	root := "1700000000.032000"
	spoof := botEvent("B0SPOOF", botRiley, "A0AAAAAA2", "1700000000.032100", root, "pretending to be Riley", nil)
	if _, handled := h.admitViaHandler(t, spoof, 0); !handled {
		t.Fatal("spoof post not handled")
	}
	h.wait()

	followup := humanMessage("1700000000.032200", "any update?")
	followup.ThreadTS = root
	if _, handled := h.admitViaHandler(t, followup, 0); !handled {
		t.Fatal("follow-up not handled")
	}
	h.wait()

	calls := gc.sessionCalls()
	got := callTargets(calls)
	if len(calls) != 1 || !got["ollie"] || got["riley"] {
		t.Fatalf("spoof-enrollment targets = %v (%d calls), want configured ambient Ollie only: %+v", got, len(calls), calls)
	}
}
