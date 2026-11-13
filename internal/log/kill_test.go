package log

// Slice-2 fault-injection acceptance: a subprocess is SIGKILLed mid-produce;
// after reopen the log must contain every record the child acknowledged and
// no torn records at all (invariant 3, ADR-003).

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const killChildEnv = "SLIPWAY_LOG_KILL_CHILD"
const killDirEnv = "SLIPWAY_LOG_KILL_DIR"

// TestFileLogSurvivesKill9 is the parent side. It starts a child process
// that appends records (fsync'd) and prints each acknowledged offset to
// stdout; once enough acks are observed it kills the child with SIGKILL,
// reopens the log, and verifies integrity.
func TestFileLogSurvivesKill9(t *testing.T) {
	if os.Getenv(killChildEnv) == "1" {
		killChildMode(t)
		return
	}

	const killAfter = 37
	dir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run", "^TestFileLogSurvivesKill9$", "-test.v")
	cmd.Env = append(os.Environ(), killChildEnv+"=1", killDirEnv+"="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	acks := 0
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "ACK ") {
			acks++
			if acks == killAfter {
				_ = cmd.Process.Signal(syscall.SIGKILL)
				break
			}
		}
	}
	_ = cmd.Wait() // reap; exit status is expected to be signal-killed

	if acks < killAfter {
		t.Fatalf("child acknowledged only %d records before kill, want >= %d", acks, killAfter)
	}

	l, err := Open(filepath.Join(dir), WithSyncOnAppend(true))
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer func() { _ = l.Close() }()

	hw := l.HighWatermark()
	if hw < int64(killAfter) {
		t.Fatalf("hwm = %d after kill, want >= %d acknowledged records", hw, killAfter)
	}
	recs, err := l.Fetch(0, int(hw))
	if err != nil {
		t.Fatalf("fetch recovered log: %v", err)
	}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("record %d: offset = %d (gap in recovered log)", i, r.Offset)
		}
		want := fmt.Sprintf("v%d", i)
		if string(r.Value) != want {
			t.Fatalf("record %d: value = %q, want %q (torn/corrupt record visible)", i, r.Value, want)
		}
	}
}

// killChildMode is the child side: append records with fsync and print
// "ACK <offset>" after each fsync. Runs until killed.
func killChildMode(t *testing.T) {
	dir := os.Getenv(killDirEnv)
	l, err := Open(dir, WithSyncOnAppend(true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open: %v\n", err)
		os.Exit(1)
	}
	for i := 0; ; i++ {
		off := l.Append([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
		fmt.Printf("ACK %d\n", off)
		time.Sleep(time.Millisecond)
	}
}
