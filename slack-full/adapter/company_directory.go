package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sync"
)

// CompanyAgent is one directory-listed agent: a stable lowercase slug name
// bound to its Slack identity app (app_id) and the app's bot user id — the
// agent's real, mentionable <@U…> identity. Mirrors the on-disk shape
// written by `gc slack import-company-directory` (schema at
// docs/company-rooms.md, "Directory Contract"); the JSON tags are the
// contract with the CLI writer.
type CompanyAgent struct {
	Name      string `json:"name"`
	AppID     string `json:"app_id"`
	BotUserID string `json:"bot_user_id"`
}

// CompanyRoom is one directory-listed room: a Slack channel keyed by
// (team_id, channel_id) with its wake policy. Members/AmbientWake/
// MentionWake carry concrete agent-name lists — the "*" wildcards in the
// source TOML are expanded to the full member set at import time and MUST
// be absent from the normalized JSON (the Go loader rejects any remaining
// wildcard, fail-closed).
type CompanyRoom struct {
	Name        string   `json:"name"`
	TeamID      string   `json:"team_id"`
	ChannelID   string   `json:"channel_id"`
	Members     []string `json:"members"`
	AmbientWake []string `json:"ambient_wake"`
	MentionWake []string `json:"mention_wake"`
}

// companyDirectoryFile is the on-disk JSON envelope for
// company_directory.json. Kept separate from CompanyDirectory so the
// public type can expose validated indexes rather than raw slices.
type companyDirectoryFile struct {
	SchemaVersion int            `json:"schema_version"`
	SourceSHA256  string         `json:"source_sha256"`
	ImportedAt    string         `json:"imported_at"`
	Agents        []CompanyAgent `json:"agents"`
	Rooms         []CompanyRoom  `json:"rooms"`
	// DMAllowedHumans is the optional directory-wide DM allowlist (Phase 4,
	// D-DM2). A pointer so the loader can distinguish "key absent" (nil =
	// all workspace humans allowed) from "key present but empty" (non-nil
	// empty = nobody allowed). Fail-closed allowlist semantics.
	DMAllowedHumans *[]string `json:"dm_allowed_humans,omitempty"`
}

// CompanyDirectory is a parsed, validated, and normalized view of
// company_directory.json. All indexes are built once at parse time and the
// value is immutable afterwards, so a *CompanyDirectory can be read
// concurrently without locking; the snapshot holder swaps whole values.
type CompanyDirectory struct {
	SchemaVersion int
	SourceSHA256  string
	ImportedAt    string

	agents []CompanyAgent
	rooms  []CompanyRoom

	agentsByName      map[string]*CompanyAgent
	agentsByBotUserID map[string]*CompanyAgent
	agentsByAppID     map[string]*CompanyAgent
	roomsByName       map[string]*CompanyRoom
	roomsByChannel    map[string]*CompanyRoom

	// dmAllowedHumans is the parsed DM allowlist (Phase 4). nil ⇒ the
	// dm_allowed_humans key was absent (all workspace humans allowed); a
	// non-nil (possibly empty) set ⇒ allowlist mode, where only listed Slack
	// user ids are allowed and an empty set allows nobody. Fail-closed.
	dmAllowedHumans map[string]bool
	dmAllowlistMode bool
}

// companyDirectorySchemaVersion is the only directory schema this build
// understands. A different value is rejected (fail-closed) rather than
// best-effort parsed; the schema gate is intentionally strict even though
// unknown object fields are tolerated for forward-compat.
const companyDirectorySchemaVersion = 1

// companySlugRE matches a lowercase stable slug: lowercase alphanumerics in
// hyphen-separated groups, with no leading/trailing/double hyphen.
var companySlugRE = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

func isCompanySlug(s string) bool {
	return companySlugRE.MatchString(s)
}

func companyChannelKey(teamID, channelID string) string {
	return teamID + ":" + channelID
}

// ParseCompanyDirectory decodes and validates the normalized directory
// JSON, returning a ready-to-serve snapshot. Validation is fail-closed:
// any duplicate or unknown reference, wildcard survivor, malformed name, or
// wake list that is not a subset of members makes the whole directory
// invalid. An empty agents/rooms document is valid (an inert directory).
func ParseCompanyDirectory(data []byte) (*CompanyDirectory, error) {
	var file companyDirectoryFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode company directory: %w", err)
	}
	if file.SchemaVersion != companyDirectorySchemaVersion {
		return nil, fmt.Errorf("company directory: unsupported schema_version %d (want %d)", file.SchemaVersion, companyDirectorySchemaVersion)
	}

	d := &CompanyDirectory{
		SchemaVersion:     file.SchemaVersion,
		SourceSHA256:      file.SourceSHA256,
		ImportedAt:        file.ImportedAt,
		agents:            make([]CompanyAgent, 0, len(file.Agents)),
		rooms:             make([]CompanyRoom, 0, len(file.Rooms)),
		agentsByName:      make(map[string]*CompanyAgent, len(file.Agents)),
		agentsByBotUserID: make(map[string]*CompanyAgent, len(file.Agents)),
		agentsByAppID:     make(map[string]*CompanyAgent, len(file.Agents)),
		roomsByName:       make(map[string]*CompanyRoom, len(file.Rooms)),
		roomsByChannel:    make(map[string]*CompanyRoom, len(file.Rooms)),
	}
	if file.DMAllowedHumans != nil {
		// Allowlist mode: the key is present (even if empty). Normalize into a
		// set; an empty set allows nobody. Absent (nil) leaves dmAllowlistMode
		// false so all workspace humans are allowed.
		d.dmAllowlistMode = true
		d.dmAllowedHumans = make(map[string]bool, len(*file.DMAllowedHumans))
		for _, u := range *file.DMAllowedHumans {
			if u == "" {
				continue
			}
			d.dmAllowedHumans[u] = true
		}
	}

	seenAppID := make(map[string]string, len(file.Agents))     // app_id -> agent name
	seenBotUserID := make(map[string]string, len(file.Agents)) // bot_user_id -> agent name
	for i := range file.Agents {
		a := file.Agents[i]
		if !isCompanySlug(a.Name) {
			return nil, fmt.Errorf("company directory: agent[%d] name %q is not a lowercase slug", i, a.Name)
		}
		if a.AppID == "" {
			return nil, fmt.Errorf("company directory: agent %q missing app_id", a.Name)
		}
		if a.BotUserID == "" {
			return nil, fmt.Errorf("company directory: agent %q missing bot_user_id", a.Name)
		}
		if _, dup := d.agentsByName[a.Name]; dup {
			return nil, fmt.Errorf("company directory: duplicate agent name %q", a.Name)
		}
		if prev, dup := seenAppID[a.AppID]; dup {
			return nil, fmt.Errorf("company directory: duplicate app_id %q (agents %q and %q)", a.AppID, prev, a.Name)
		}
		if prev, dup := seenBotUserID[a.BotUserID]; dup {
			return nil, fmt.Errorf("company directory: duplicate bot_user_id %q (agents %q and %q)", a.BotUserID, prev, a.Name)
		}
		d.agents = append(d.agents, a)
		// Index by name eagerly so downstream room validation can resolve
		// member references. The bot-user-id index is filled after the
		// slice is final so the stored pointers stay stable.
		d.agentsByName[a.Name] = nil
		seenAppID[a.AppID] = a.Name
		seenBotUserID[a.BotUserID] = a.Name
	}
	for i := range d.agents {
		a := &d.agents[i]
		d.agentsByName[a.Name] = a
		d.agentsByBotUserID[a.BotUserID] = a
		d.agentsByAppID[a.AppID] = a
	}

	seenChannel := make(map[string]string, len(file.Rooms)) // (team,channel) -> room name
	for i := range file.Rooms {
		r := file.Rooms[i]
		if !isCompanySlug(r.Name) {
			return nil, fmt.Errorf("company directory: room[%d] name %q is not a lowercase slug", i, r.Name)
		}
		if r.TeamID == "" {
			return nil, fmt.Errorf("company directory: room %q missing team_id", r.Name)
		}
		if r.ChannelID == "" {
			return nil, fmt.Errorf("company directory: room %q missing channel_id", r.Name)
		}
		if _, dup := d.roomsByName[r.Name]; dup {
			return nil, fmt.Errorf("company directory: duplicate room name %q", r.Name)
		}
		ck := companyChannelKey(r.TeamID, r.ChannelID)
		if prev, dup := seenChannel[ck]; dup {
			return nil, fmt.Errorf("company directory: duplicate (team_id, channel_id) (%s,%s) for rooms %q and %q", r.TeamID, r.ChannelID, prev, r.Name)
		}
		if err := validateRoomMembership(&r, d.agentsByName); err != nil {
			return nil, err
		}
		d.rooms = append(d.rooms, r)
		d.roomsByName[r.Name] = nil
		seenChannel[ck] = r.Name
	}
	for i := range d.rooms {
		r := &d.rooms[i]
		d.roomsByName[r.Name] = r
		d.roomsByChannel[companyChannelKey(r.TeamID, r.ChannelID)] = r
	}

	return d, nil
}

// validateRoomMembership enforces, per room: no surviving wildcards, every
// member is a known agent, no member is listed twice, and both wake lists
// are subsets of the member set.
func validateRoomMembership(r *CompanyRoom, agents map[string]*CompanyAgent) error {
	members := make(map[string]bool, len(r.Members))
	for _, m := range r.Members {
		if m == "*" {
			return fmt.Errorf("company directory: room %q members contains wildcard %q (must be expanded before the normalized JSON)", r.Name, m)
		}
		if _, ok := agents[m]; !ok {
			return fmt.Errorf("company directory: room %q members references unknown agent %q", r.Name, m)
		}
		if members[m] {
			return fmt.Errorf("company directory: room %q members lists agent %q twice", r.Name, m)
		}
		members[m] = true
	}
	if err := validateWakeList(r.Name, "ambient_wake", r.AmbientWake, members); err != nil {
		return err
	}
	return validateWakeList(r.Name, "mention_wake", r.MentionWake, members)
}

func validateWakeList(room, field string, list []string, members map[string]bool) error {
	seen := make(map[string]bool, len(list))
	for _, name := range list {
		if name == "*" {
			return fmt.Errorf("company directory: room %q %s contains wildcard %q (must be expanded before the normalized JSON)", room, field, name)
		}
		if !members[name] {
			return fmt.Errorf("company directory: room %q %s references %q which is not a member", room, field, name)
		}
		if seen[name] {
			return fmt.Errorf("company directory: room %q %s lists %q twice", room, field, name)
		}
		seen[name] = true
	}
	return nil
}

// Agents returns the directory's agents. The slice is owned by the
// directory; callers must not mutate it.
func (d *CompanyDirectory) Agents() []CompanyAgent {
	if d == nil {
		return nil
	}
	return d.agents
}

// Rooms returns the directory's rooms. The slice is owned by the
// directory; callers must not mutate it.
func (d *CompanyDirectory) Rooms() []CompanyRoom {
	if d == nil {
		return nil
	}
	return d.rooms
}

// RoomByChannel resolves the room for a Slack (team_id, channel_id). A
// nil directory reports no room, so callers fall through to the legacy
// path.
func (d *CompanyDirectory) RoomByChannel(teamID, channelID string) (*CompanyRoom, bool) {
	if d == nil {
		return nil, false
	}
	r, ok := d.roomsByChannel[companyChannelKey(teamID, channelID)]
	return r, ok
}

// AgentByBotUserID resolves the agent whose bot user id equals id — the
// match used to test a native mention against the directory.
func (d *CompanyDirectory) AgentByBotUserID(id string) (*CompanyAgent, bool) {
	if d == nil || id == "" {
		return nil, false
	}
	a, ok := d.agentsByBotUserID[id]
	return a, ok
}

// AgentByAppID resolves the agent whose Slack app_id equals id — the join
// used to derive a DM's owner agent from the delivering app's api_app_id
// (Phase 4). app_id is unique per directory (ParseCompanyDirectory rejects
// duplicates), so this is a single-agent resolution.
func (d *CompanyDirectory) AgentByAppID(id string) (*CompanyAgent, bool) {
	if d == nil || id == "" {
		return nil, false
	}
	a, ok := d.agentsByAppID[id]
	return a, ok
}

// DMAuthorAllowed reports whether a DM author identified by Slack user id is
// allowed to wake the bound agent under the directory's DM policy (Phase 4,
// D-DM2). When the directory carries no dm_allowed_humans key, every
// workspace human is allowed. When the key is present (even empty), only
// listed user ids are allowed and an empty list allows nobody. The caller has
// already established that the author is a workspace human in the bound team.
func (d *CompanyDirectory) DMAuthorAllowed(userID string) bool {
	if d == nil {
		return false
	}
	if !d.dmAllowlistMode {
		return true
	}
	return d.dmAllowedHumans[userID]
}

// DMAllowlistActive reports whether the directory is in DM allowlist mode (the
// dm_allowed_humans key is present, even if empty). Used by the mpim reminder
// to decide whether an unlisted excerpted author downgrades the provenance line
// to human_root_unlisted (spec §Semantics); when the allowlist is absent every
// workspace human is allowed and no downgrade applies.
func (d *CompanyDirectory) DMAllowlistActive() bool {
	return d != nil && d.dmAllowlistMode
}

// AgentByName resolves the agent for a directory slug name.
func (d *CompanyDirectory) AgentByName(name string) (*CompanyAgent, bool) {
	if d == nil {
		return nil, false
	}
	a, ok := d.agentsByName[name]
	return a, ok
}

// roomByName resolves a room by its slug name; used by binding validation.
func (d *CompanyDirectory) roomByName(name string) (*CompanyRoom, bool) {
	if d == nil {
		return nil, false
	}
	r, ok := d.roomsByName[name]
	return r, ok
}

// IsMember reports whether agent is a member of room.
func (d *CompanyDirectory) IsMember(room *CompanyRoom, agent string) bool {
	if d == nil || room == nil {
		return false
	}
	for _, m := range room.Members {
		if m == agent {
			return true
		}
	}
	return false
}

// IsMentionEligible reports whether agent may be woken by a native mention
// in room (agent ∈ room.MentionWake).
func (d *CompanyDirectory) IsMentionEligible(room *CompanyRoom, agent string) bool {
	if d == nil || room == nil {
		return false
	}
	for _, m := range room.MentionWake {
		if m == agent {
			return true
		}
	}
	return false
}

// readCompanyRegistryBytes reads a company registry file, sharing the
// symlink-rejecting open and size cap with the six atomic registries. It
// returns exists=false (with a nil error) when the file is absent, which
// both company registries treat as the normal "not imported yet" state.
func readCompanyRegistryBytes(path string) (data []byte, exists bool, err error) {
	if path == "" {
		return nil, false, nil
	}
	f, err := openRegistryFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	data, err = io.ReadAll(io.LimitReader(f, maxRegistryBytes+1))
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}
	if int64(len(data)) > maxRegistryBytes {
		return nil, false, fmt.Errorf("company registry file %s exceeds %d bytes", path, maxRegistryBytes)
	}
	return data, true, nil
}

// companyDirectoryStore is the in-memory snapshot holder for the company
// directory. Deliberate divergence from the six atomic registries: a load
// or reload error is NEVER fatal — company routing simply goes dark while
// the adapter keeps serving legacy traffic. A nil snapshot means "no
// company routing".
type companyDirectoryStore struct {
	mu   sync.RWMutex
	snap *CompanyDirectory
}

// Snapshot returns the current directory snapshot, or nil when company
// routing is disabled (never imported, or the file failed to load).
func (s *companyDirectoryStore) Snapshot() *CompanyDirectory {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

func (s *companyDirectoryStore) set(d *CompanyDirectory) {
	s.mu.Lock()
	s.snap = d
	s.mu.Unlock()
}

func (s *companyDirectoryStore) parse(path string) (*CompanyDirectory, error) {
	data, exists, err := readCompanyRegistryBytes(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		// Absent directory is the normal disabled state, not an error.
		return nil, nil
	}
	return ParseCompanyDirectory(data)
}

// Load installs the directory snapshot at startup. Unlike the six atomic
// registries, a corrupt or semantically invalid file is not fatal: the
// validation error is logged and surfaced, a nil snapshot is installed
// (company routing disabled), and the adapter keeps running. An absent
// file installs a nil snapshot with no error. The returned error is for
// surfacing only; the caller MUST NOT treat it as fatal.
func (s *companyDirectoryStore) Load(path string) error {
	d, err := s.parse(path)
	if err != nil {
		log.Printf("WARN: company directory: load from %q failed: %v; installing nil snapshot (company routing disabled)", path, err)
		s.set(nil)
		return err
	}
	s.set(d)
	return nil
}

// StageReload re-reads the directory on SIGHUP, outside the six-registry
// atomic set. A valid replacement (including an absent file, which
// disables routing) is installed; a malformed or semantically invalid file
// retains the last-known-good snapshot and surfaces the error, so a bad
// edit never blanks a live directory.
func (s *companyDirectoryStore) StageReload(path string) error {
	d, err := s.parse(path)
	if err != nil {
		log.Printf("WARN: company directory: reload from %q failed: %v; retaining last-known-good snapshot", path, err)
		return err
	}
	s.set(d)
	return nil
}
