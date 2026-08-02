// Package targetinventory discovers browser-safe local runtime candidates.
package targetinventory

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/personastack/personastack-connector/internal/externalagentprotocol"
	connectorruntime "github.com/personastack/personastack-connector/internal/runtime"
)

type account struct {
	username string
	homeDir  string
	uid      int
	gid      int
	groupIDs []int
}

// ResolvedTarget is the local account and profile selected by PersonaStack.
// It is resolved for a single operation and must never be saved in Binding.
type ResolvedTarget struct {
	Username   string
	HomeDir    string
	UID        int
	GID        int
	GroupIDs   []int
	HermesHome string
}

var currentUser = user.Current
var readFile = os.ReadFile
var readDir = os.ReadDir
var stat = os.Stat
var lstat = os.Lstat
var effectiveUID = os.Geteuid

// Discover returns candidates visible to the effective Connector process. A
// non-root Connector sees only its own home. Root also sees root and regular
// local accounts. Errors for inaccessible individual homes are omitted so an
// unprivileged install remains usable.
func Discover(kind connectorruntime.AdapterKind, installationSecret ...string) (externalagentprotocol.TargetInventoryPayload, []error) {
	secret := ""
	if len(installationSecret) > 0 {
		secret = strings.TrimSpace(installationSecret[0])
	}
	accounts, warnings := discoverAccounts()
	result := externalagentprotocol.TargetInventoryPayload{Accounts: make([]externalagentprotocol.RuntimeAccountCandidate, 0, len(accounts))}
	for _, account := range accounts {
		profiles, profileWarnings := discoverProfiles(account, kind, secret)
		warnings = append(warnings, profileWarnings...)
		if len(profiles) == 0 {
			continue
		}
		result.Accounts = append(result.Accounts, externalagentprotocol.RuntimeAccountCandidate{
			CandidateID: accountID(account, secret),
			Label:       account.username,
			Profiles:    profiles,
		})
	}
	return result, warnings
}

// Resolve verifies that an API-provided target is still currently discoverable.
// It returns the user's home and the Hermes profile directory without persisting
// either value in Connector state or transmitting it to PersonaStack.
func Resolve(kind connectorruntime.AdapterKind, target *externalagentprotocol.RuntimeTarget, installationSecret ...string) (ResolvedTarget, error) {
	if target == nil || target.RuntimeKind != protocolRuntimeKind(kind) {
		return ResolvedTarget{}, fmt.Errorf("runtime target required")
	}
	secret := ""
	if len(installationSecret) > 0 {
		secret = strings.TrimSpace(installationSecret[0])
	}
	inventory, _ := Discover(kind, secret)
	for _, accountCandidate := range inventory.Accounts {
		if accountCandidate.CandidateID != strings.TrimSpace(target.AccountCandidateID) {
			continue
		}
		for _, profile := range accountCandidate.Profiles {
			if profile.CandidateID != strings.TrimSpace(target.ProfileCandidateID) {
				continue
			}
			accounts, _ := discoverAccounts()
			for _, candidate := range accounts {
				if accountID(candidate, secret) != accountCandidate.CandidateID {
					continue
				}
				runtimeHome := profileHome(candidate, kind, profile.CandidateID, secret)
				if err := validateResolvedTarget(candidate, kind, runtimeHome); err != nil {
					return ResolvedTarget{}, err
				}
				return ResolvedTarget{
					Username:   candidate.username,
					HomeDir:    candidate.homeDir,
					UID:        candidate.uid,
					GID:        candidate.gid,
					GroupIDs:   append([]int(nil), candidate.groupIDs...),
					HermesHome: runtimeHome,
				}, nil
			}
		}
	}
	return ResolvedTarget{}, fmt.Errorf("selected runtime target is no longer available")
}

func validateResolvedTarget(candidate account, kind connectorruntime.AdapterKind, runtimeHome string) error {
	paths := []string{candidate.homeDir}
	switch kind {
	case connectorruntime.AdapterKindHermes:
		paths = append(paths, runtimeHome)
	case connectorruntime.AdapterKindOpenClaw:
		paths = append(paths, filepath.Join(candidate.homeDir, ".openclaw"))
	}
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("selected runtime path unavailable")
		}
		info, err := lstat(path)
		if err != nil {
			return fmt.Errorf("inspect selected runtime path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("selected runtime path is unsafe")
		}
		if effectiveUID() == 0 && !ownedByAccount(info, candidate.uid) {
			return fmt.Errorf("selected runtime path ownership cannot be established")
		}
	}
	return nil
}

func ownedByAccount(info os.FileInfo, uid int) bool {
	if info == nil || uid < 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == uid
}

func discoverAccounts() ([]account, []error) {
	if effectiveUID() != 0 {
		current, err := currentUser()
		if err != nil {
			return nil, []error{fmt.Errorf("resolve current user: %w", err)}
		}
		uid, _ := strconv.Atoi(current.Uid)
		gid, _ := strconv.Atoi(current.Gid)
		return []account{{username: strings.TrimSpace(current.Username), homeDir: strings.TrimSpace(current.HomeDir), uid: uid, gid: gid, groupIDs: groupIDsForUser(current, gid)}}, nil
	}
	raw, err := readFile("/etc/passwd")
	if err != nil {
		return nil, []error{fmt.Errorf("list local users: %w", err)}
	}
	minimumUID := 1000
	if runtime.GOOS == "darwin" {
		minimumUID = 500
	}
	seen := map[string]struct{}{}
	accounts := []account{}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) < 7 {
			continue
		}
		uid, parseErr := strconv.Atoi(fields[2])
		if parseErr != nil || (uid != 0 && uid < minimumUID) {
			continue
		}
		username := strings.TrimSpace(fields[0])
		homeDir := strings.TrimSpace(fields[5])
		if username == "" || homeDir == "" || homeDir == "/" {
			continue
		}
		key := username + "\x00" + homeDir
		if _, found := seen[key]; found {
			continue
		}
		seen[key] = struct{}{}
		gid, gidErr := strconv.Atoi(fields[3])
		if gidErr != nil {
			continue
		}
		accounts = append(accounts, account{username: username, homeDir: homeDir, uid: uid, gid: gid, groupIDs: groupIDsForUID(uid, gid)})
	}
	sort.Slice(accounts, func(i int, j int) bool { return accounts[i].username < accounts[j].username })
	return accounts, nil
}

func groupIDsForUser(current *user.User, primaryGID int) []int {
	if current == nil {
		return []int{primaryGID}
	}
	groups, err := current.GroupIds()
	if err != nil {
		return []int{primaryGID}
	}
	return parseGroupIDs(groups, primaryGID)
}

func groupIDsForUID(uid int, primaryGID int) []int {
	current, err := user.LookupId(strconv.Itoa(uid))
	if err != nil {
		return []int{primaryGID}
	}
	return groupIDsForUser(current, primaryGID)
}

func parseGroupIDs(values []string, primaryGID int) []int {
	seen := map[int]struct{}{primaryGID: {}}
	groups := []int{primaryGID}
	for _, value := range values {
		groupID, err := strconv.Atoi(value)
		if err != nil || groupID < 0 {
			continue
		}
		if _, found := seen[groupID]; found {
			continue
		}
		seen[groupID] = struct{}{}
		groups = append(groups, groupID)
	}
	sort.Ints(groups)
	return groups
}

func discoverProfiles(candidate account, kind connectorruntime.AdapterKind, installationSecret string) ([]externalagentprotocol.RuntimeProfileCandidate, []error) {
	if strings.TrimSpace(candidate.homeDir) == "" {
		return nil, nil
	}
	switch kind {
	case connectorruntime.AdapterKindHermes:
		base := filepath.Join(candidate.homeDir, ".hermes")
		if _, err := stat(base); err != nil {
			if os.IsNotExist(err) || os.IsPermission(err) {
				return nil, nil
			}
			return nil, []error{fmt.Errorf("inspect Hermes home for %s: %w", candidate.username, err)}
		}
		profiles := []externalagentprotocol.RuntimeProfileCandidate{{CandidateID: profileID(candidate, "default", installationSecret), Label: "Default", RuntimeKind: externalagentprotocol.RuntimeKindHermes}}
		entries, err := readDir(filepath.Join(base, "profiles"))
		if err != nil {
			if os.IsNotExist(err) || os.IsPermission(err) {
				return profiles, nil
			}
			return profiles, []error{fmt.Errorf("list Hermes profiles for %s: %w", candidate.username, err)}
		}
		for _, entry := range entries {
			if !entry.IsDir() || strings.TrimSpace(entry.Name()) == "" {
				continue
			}
			profiles = append(profiles, externalagentprotocol.RuntimeProfileCandidate{CandidateID: profileID(candidate, entry.Name(), installationSecret), Label: entry.Name(), RuntimeKind: externalagentprotocol.RuntimeKindHermes})
		}
		sort.Slice(profiles[1:], func(i int, j int) bool { return profiles[i+1].Label < profiles[j+1].Label })
		return profiles, nil
	case connectorruntime.AdapterKindOpenClaw:
		if _, err := stat(filepath.Join(candidate.homeDir, ".openclaw")); err != nil {
			if os.IsNotExist(err) || os.IsPermission(err) {
				return nil, nil
			}
			return nil, []error{fmt.Errorf("inspect OpenClaw home for %s: %w", candidate.username, err)}
		}
		return []externalagentprotocol.RuntimeProfileCandidate{{CandidateID: profileID(candidate, "default", installationSecret), Label: "Default", RuntimeKind: externalagentprotocol.RuntimeKindOpenClaw}}, nil
	default:
		return nil, nil
	}
}

func accountID(candidate account, installationSecret string) string {
	return opaqueID(installationSecret, "account", candidate.username, candidate.homeDir)
}
func profileID(candidate account, name string, installationSecret string) string {
	return opaqueID(installationSecret, "profile", candidate.username, candidate.homeDir, name)
}

func profileHome(candidate account, kind connectorruntime.AdapterKind, candidateID string, installationSecret string) string {
	if kind != connectorruntime.AdapterKindHermes {
		return ""
	}
	if candidateID == profileID(candidate, "default", installationSecret) {
		return filepath.Join(candidate.homeDir, ".hermes")
	}
	entries, _ := readDir(filepath.Join(candidate.homeDir, ".hermes", "profiles"))
	for _, entry := range entries {
		if entry.IsDir() && candidateID == profileID(candidate, entry.Name(), installationSecret) {
			return filepath.Join(candidate.homeDir, ".hermes", "profiles", entry.Name())
		}
	}
	return ""
}

func opaqueID(parts ...string) string {
	secret := ""
	if len(parts) > 0 {
		secret = parts[0]
		parts = parts[1:]
	}
	key := []byte(secret)
	if len(key) == 0 {
		key = []byte("personastack-connector-unpaired")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(strings.Join(parts, "\x00")))
	return "rt_" + hex.EncodeToString(mac.Sum(nil)[:16])
}

func protocolRuntimeKind(kind connectorruntime.AdapterKind) externalagentprotocol.RuntimeKind {
	switch kind {
	case connectorruntime.AdapterKindHermes:
		return externalagentprotocol.RuntimeKindHermes
	case connectorruntime.AdapterKindOpenClaw:
		return externalagentprotocol.RuntimeKindOpenClaw
	default:
		return ""
	}
}
