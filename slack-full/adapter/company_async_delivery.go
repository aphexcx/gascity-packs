package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	companyAsyncResultTimeout        = 4 * time.Minute
	companyAsyncSuccessEvent         = "request.result.session.message"
	companyAsyncFailureEvent         = "request.failed"
	companyAsyncMessageOperation     = "session.message"
	companyDetailAwaitingMessage     = "awaiting_session_message_result"
	companyMaxAsyncResponseBodyBytes = 4096
	companyMaxAsyncDetailBytes       = 4096
	companyMaxAsyncRequestIDBytes    = 128
)

type companyPostResult struct {
	disposition postDisposition
	detail      string
	requestID   string
	eventCursor string
}

type companyAsyncAccepted struct {
	Status      string `json:"status"`
	RequestID   string `json:"request_id"`
	EventCursor string `json:"event_cursor"`
}

type companyAsyncEnvelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type companyAsyncFailure struct {
	RequestID    string `json:"request_id"`
	Operation    string `json:"operation"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

func normalizeCompanyAsyncAcceptance(accepted companyAsyncAccepted) (requestID, eventCursor string, err error) {
	if accepted.Status != "accepted" {
		return "", "", fmt.Errorf("async acceptance status %q, want accepted", accepted.Status)
	}
	requestID = strings.TrimSpace(accepted.RequestID)
	if requestID == "" {
		return "", "", fmt.Errorf("async acceptance missing request_id")
	}
	if requestID != accepted.RequestID || len(requestID) > companyMaxAsyncRequestIDBytes {
		return "", "", fmt.Errorf("async acceptance invalid request_id")
	}
	for i := 0; i < len(requestID); i++ {
		if requestID[i] < '!' || requestID[i] > '~' {
			return "", "", fmt.Errorf("async acceptance invalid request_id")
		}
	}

	eventCursor = strings.TrimSpace(accepted.EventCursor)
	if eventCursor == "" {
		eventCursor = "0"
	} else if eventCursor != accepted.EventCursor {
		return "", "", fmt.Errorf("async acceptance invalid event_cursor")
	}
	seq, parseErr := strconv.ParseUint(eventCursor, 10, 64)
	if parseErr != nil {
		return "", "", fmt.Errorf("async acceptance invalid event_cursor")
	}
	return requestID, strconv.FormatUint(seq, 10), nil
}

func boundCompanyAsyncDetail(detail string) string {
	detail = strings.TrimSpace(detail)
	if len(detail) <= companyMaxAsyncDetailBytes {
		return detail
	}
	const suffix = "..."
	detail = detail[:companyMaxAsyncDetailBytes-len(suffix)]
	for !utf8.ValidString(detail) {
		detail = detail[:len(detail)-1]
	}
	return detail + suffix
}

// mergeCompanyTargetResult makes target settlement monotonic across a stale
// generation retry or overlapping recovery watcher. A terminal target never
// moves backward, and the first durable async correlation cannot be erased or
// replaced by a late uncorrelated result.
func mergeCompanyTargetResult(targets map[string]TargetDelivery, key string, next TargetDelivery) {
	current, ok := targets[key]
	if !ok {
		targets[key] = next
		return
	}
	if current.Status == companyTargetDelivered || current.Status == companyTargetFailed {
		return
	}
	if current.RequestID != "" && (next.RequestID == "" || next.RequestID != current.RequestID) {
		return
	}
	targets[key] = next
}

// companyTargetAPI resolves the supervisor base and city for a frozen target.
// City-qualified bindings use the same mapping for POST and event recovery.
func (g *companyGateway) companyTargetAPI(td TargetDelivery) (string, string, error) {
	targetCity, apiBase := td.City, g.cfg.gcAPIBase
	if targetCity == "" {
		targetCity = g.cfg.cityName
	} else if targetCity != g.cfg.cityName {
		mapped, ok := g.cfg.companyCityAPIs[targetCity]
		if !ok {
			return "", "", fmt.Errorf("no SLACK_COMPANY_CITY_APIS entry for city %q", targetCity)
		}
		apiBase = mapped
	}
	return targetCity, apiBase, nil
}

// persistCompanyMessageAcceptance is the durability boundary between the 202
// response and the SSE wait. Recovery sees the correlation before this worker
// opens the stream, so an adapter restart resumes the request rather than
// submitting the message a second time.
func (g *companyGateway) persistCompanyMessageAcceptance(r *IngressReceipt, key string, td *TargetDelivery, accepted companyPostResult) error {
	td.RequestID = accepted.requestID
	td.EventCursor = accepted.eventCursor
	td.Status = companyTargetPending
	td.Detail = companyDetailAwaitingMessage
	snapshot := *td
	return g.commitReceipt(r, func(cur *IngressReceipt) {
		if cur.Targets == nil {
			cur.Targets = make(map[string]TargetDelivery, 1)
		}
		mergeCompanyTargetResult(cur.Targets, key, snapshot)
		*td = cur.Targets[key]
		cur.Status, cur.Reason = computeReceiptStatus(cur.Targets)
	})
}

func (g *companyGateway) settleCompanyAsyncTarget(td *TargetDelivery) companyPostResult {
	if td.Status == companyTargetDelivered {
		return companyPostResult{disposition: postDelivered}
	}
	if td.Status == companyTargetFailed {
		return companyPostResult{disposition: postDefinitive, detail: td.Detail}
	}
	result := g.awaitCompanyMessageResult(*td)
	td.UpdatedAt = g.now().UTC()
	switch result.disposition {
	case postDelivered:
		td.Status = companyTargetDelivered
		td.Detail = ""
	case postRetryable:
		td.Status = companyTargetPending
		td.Detail = result.detail
		g.deliveryFailures.Add(1)
	default:
		td.Status = companyTargetFailed
		td.Detail = result.detail
		g.deliveryFailures.Add(1)
	}
	return result
}

// awaitCompanyMessageResult watches the target city's durable event stream for
// the terminal result of one already-accepted session.message request. Stream
// interruption is retryable and never causes another POST because RequestID
// remains on the target.
func (g *companyGateway) awaitCompanyMessageResult(td TargetDelivery) companyPostResult {
	if strings.TrimSpace(td.RequestID) == "" {
		return companyPostResult{disposition: postDefinitive, detail: "missing async request_id"}
	}
	targetCity, apiBase, err := g.companyTargetAPI(td)
	if err != nil {
		return companyPostResult{disposition: postDefinitive, detail: err.Error()}
	}
	cursor := strings.TrimSpace(td.EventCursor)
	if cursor == "" {
		cursor = "0"
	}
	target := fmt.Sprintf("%s/v0/city/%s/events/stream?after_seq=%s",
		strings.TrimRight(apiBase, "/"), url.PathEscape(targetCity), url.QueryEscape(cursor))
	timeout := g.asyncResultTimeout
	if timeout <= 0 {
		timeout = companyAsyncResultTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return companyPostResult{disposition: postDefinitive, detail: "build event-stream request: " + err.Error()}
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-GC-Request", companyDeliverRequestTag)

	client := g.eventClient
	if client == nil {
		client = &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: companyDeliverTimeout}}
	}
	resp, err := client.Do(req)
	if err != nil {
		return companyPostResult{disposition: postRetryable, detail: "event stream: " + err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, companyMaxAsyncResponseBodyBytes))
		return companyPostResult{disposition: postRetryable, detail: boundCompanyAsyncDetail(fmt.Sprintf("event stream %s: %s", resp.Status, strings.TrimSpace(string(body))))}
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var data strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if data.Len() == 0 {
				continue
			}
			result, matched := decodeCompanyMessageEvent(data.String(), td.RequestID)
			data.Reset()
			if matched {
				return result
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			part := strings.TrimPrefix(line, "data:")
			part = strings.TrimPrefix(part, " ")
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(part)
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return companyPostResult{disposition: postRetryable, detail: "event stream: " + ctx.Err().Error()}
		}
		return companyPostResult{disposition: postRetryable, detail: "event stream read: " + err.Error()}
	}
	if ctx.Err() != nil {
		return companyPostResult{disposition: postRetryable, detail: "event stream: " + ctx.Err().Error()}
	}
	return companyPostResult{disposition: postRetryable, detail: "event stream closed before request result"}
}

func decodeCompanyMessageEvent(data, requestID string) (companyPostResult, bool) {
	var env companyAsyncEnvelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		return companyPostResult{}, false
	}
	switch env.Type {
	case companyAsyncSuccessEvent:
		var payload struct {
			RequestID string `json:"request_id"`
		}
		if json.Unmarshal(env.Payload, &payload) != nil || payload.RequestID != requestID {
			return companyPostResult{}, false
		}
		return companyPostResult{disposition: postDelivered}, true
	case companyAsyncFailureEvent:
		var payload companyAsyncFailure
		if json.Unmarshal(env.Payload, &payload) != nil || payload.RequestID != requestID || payload.Operation != companyAsyncMessageOperation {
			return companyPostResult{}, false
		}
		detail := strings.TrimSpace(payload.ErrorCode)
		if payload.ErrorMessage != "" {
			if detail != "" {
				detail += ": "
			}
			detail += payload.ErrorMessage
		}
		if detail == "" {
			detail = "session.message failed"
		}
		return companyPostResult{disposition: postDefinitive, detail: boundCompanyAsyncDetail(detail)}, true
	default:
		return companyPostResult{}, false
	}
}
