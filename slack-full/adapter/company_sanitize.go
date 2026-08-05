package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// company_sanitize.go — the cross-language filename/lock/nonce spec shared by
// the Python outbound side (2b) and this Go ingress side (2c). Every helper
// here is a byte-for-byte contract validated against
// slack-full/tests/fixtures/company/. Do not change the digest math without
// updating the fixtures and the Python module in lockstep.

// companyComponentSafe reports whether a filename component passes through
// the sanitizer unchanged. A component is replaced (hashed) when it contains
// a byte outside [A-Za-z0-9._-], begins with '.', equals "..", or exceeds 64
// bytes. An empty component is safe (passes through) — the callers here never
// produce one, but the rule must match the Python side exactly.
func companyComponentSafe(c string) bool {
	if len(c) > 64 || c == ".." || strings.HasPrefix(c, ".") {
		return false
	}
	for i := 0; i < len(c); i++ {
		ch := c[i]
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '.' || ch == '_' || ch == '-':
		default:
			return false
		}
	}
	return true
}

// companySanitizeComponent returns c unchanged when it is filename-safe,
// otherwise "h" + sha256hex(c)[:16]. Note the "h" prefix and 16-hex length
// are the shared spec and deliberately differ from ingress_receipts.go's
// safeStorageID (which uses a "<prefix>-<24hex>" form) — the two sanitizers
// serve different on-disk namespaces.
func companySanitizeComponent(c string) string {
	if companyComponentSafe(c) {
		return c
	}
	sum := sha256.Sum256([]byte(c))
	return "h" + hex.EncodeToString(sum[:])[:16]
}

// companyTupleDigest12 is the 12-hex disambiguating suffix over the raw
// (unsanitized) origin components joined by NUL, mirroring Go's receiptID
// digest discipline so two distinct origins can never collide even when their
// sanitized readable forms would.
func companyTupleDigest12(teamID, channelID, ts string) string {
	sum := sha256.Sum256([]byte(teamID + "\x00" + channelID + "\x00" + ts))
	return hex.EncodeToString(sum[:])[:12]
}

// companyDelegationFilename derives the delegations-registry filename for a
// posted delegation keyed by (team, channel, ts). ts is the delegation
// message's posted Slack ts (the delegation record's `ts` field), never the
// human root.
func companyDelegationFilename(teamID, channelID, ts string) string {
	return "dg-" +
		companySanitizeComponent(teamID) + "-" +
		companySanitizeComponent(channelID) + "-" +
		companySanitizeComponent(ts) + "-" +
		companyTupleDigest12(teamID, channelID, ts) + ".json"
}

// companyLockFilename builds an advisory-lock filename:
// "<label>-<sha256hex(NUL-joined key fields)[:16]>.lock". The two Phase 2
// labels are "dtuple" (delegation tuple) and "intent" (nonce).
func companyLockFilename(label string, keyFields ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(keyFields, "\x00")))
	return label + "-" + hex.EncodeToString(sum[:])[:16] + ".lock"
}

// companyFileLock is a held advisory flock(LOCK_EX). Release exactly once via
// release(); it is nil-safe so callers can defer unconditionally.
type companyFileLock struct {
	f *os.File
}

// acquireCompanyLock creates locksDir if needed and takes an exclusive
// advisory lock on locksDir/<name>, blocking until it is granted. The lock is
// the cross-process serialization primitive; generation checks on the record
// remain on top of it as defense in depth.
func acquireCompanyLock(locksDir, name string) (*companyFileLock, error) {
	if err := os.MkdirAll(locksDir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(locksDir, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &companyFileLock{f: f}, nil
}

func (l *companyFileLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
	l.f = nil
}
