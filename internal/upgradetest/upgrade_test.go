// Package upgradetest checks that work parked by the previous SDK finishes
// under this one, on the same database: the schema is created by the
// previous SDK, which enqueues and parks work (sleeping, waiting on a signal,
// waiting on a pending child, scheduled, pending) and stops; this SDK then
// starts on that database and must finish all of it, with every recorded
// step's effect happening exactly once.
//
// It runs when RUNNERQ_TEST_DSN and RUNNERQ_UPGRADE_FROM (a git ref of this
// repository: a tag, branch or commit) are set:
//
//	RUNNERQ_UPGRADE_FROM=v0.6.0 RUNNERQ_TEST_DSN=postgres://... go test ./internal/upgradetest -v
//
// The test creates and drops its own database next to the DSN's.
package upgradetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	modulePath   = "github.com/runnerq/runnerq-go"
	workloadPath = "internal/upgradetest/workload"
)

func TestUpgradeFromPreviousSDK(t *testing.T) {
	ref := os.Getenv("RUNNERQ_UPGRADE_FROM")
	baseDSN := os.Getenv("RUNNERQ_TEST_DSN")
	if ref == "" || baseDSN == "" {
		t.Skip("set RUNNERQ_UPGRADE_FROM and RUNNERQ_TEST_DSN to run the upgrade test")
	}
	root := strings.TrimSpace(run(t, "", "git", "rev-parse", "--show-toplevel"))
	commit := strings.TrimSpace(run(t, root, "git", "rev-parse", "--verify", ref+"^{commit}"))
	t.Logf("upgrading from %s (%s)", ref, commit)

	bin := t.TempDir()
	oldBin := filepath.Join(bin, "workload-previous")
	newBin := filepath.Join(bin, "workload-current")
	buildPrevious(t, root, commit, oldBin)
	run(t, root, "go", "build", "-o", newBin, "./"+workloadPath)

	dsn := freshDatabase(t, baseDSN)
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(context.Background(), `CREATE TABLE upgrade_effects (label text NOT NULL, step text NOT NULL)`)
	conn.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	state := filepath.Join(bin, "state.json")
	t.Log(run(t, "", oldBin, "-phase", "park", "-dsn", dsn, "-state", state))
	t.Log(run(t, "", newBin, "-phase", "finish", "-dsn", dsn, "-state", state))
}

// buildPrevious builds the current workload source against the SDK at
// commit, rewriting the module path in its imports if that SDK had another.
func buildPrevious(t *testing.T, root, commit, out string) {
	t.Helper()
	dir := t.TempDir()
	archive := exec.Command("git", "-C", root, "archive", "--format=tar", commit)
	extract := exec.Command("tar", "-x", "-C", dir)
	pipe, err := archive.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	extract.Stdin = pipe
	var stderr bytes.Buffer
	archive.Stderr, extract.Stderr = &stderr, &stderr
	if err := extract.Start(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Run(); err != nil {
		t.Fatalf("git archive %s: %v\n%s", commit, err, stderr.String())
	}
	if err := extract.Wait(); err != nil {
		t.Fatalf("extract %s: %v\n%s", commit, err, stderr.String())
	}

	oldModule := strings.TrimSpace(run(t, dir, "go", "list", "-m"))
	src, err := os.ReadFile(filepath.Join(root, workloadPath, "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	src = bytes.ReplaceAll(src, []byte(`"`+modulePath), []byte(`"`+oldModule))
	if err := os.MkdirAll(filepath.Join(dir, workloadPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, workloadPath, "main.go"), src, 0o600); err != nil {
		t.Fatal(err)
	}
	run(t, dir, "go", "build", "-o", out, "./"+workloadPath)
}

// freshDatabase creates an empty database on baseDSN's server, dropped when
// the test ends, and returns its DSN.
func freshDatabase(t *testing.T, baseDSN string) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "runnerq_upgrade_" + hex.EncodeToString(suffix)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		conn.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Logf("drop %s: %v", name, err)
		}
		conn.Close(ctx)
	})

	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func run(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out.String())
	}
	return out.String()
}
