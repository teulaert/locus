package peer

import (
	"testing"
	"time"
)

func info(id string) Info {
	return Info{PeerID: id, Hostname: id + "-host", Version: "0.1.0", SyncAddr: "10.0.0.1:8909"}
}

func TestObserveOnlineAndSnapshot(t *testing.T) {
	tbl := NewTable(time.Second)
	tbl.Observe("10.0.0.1:8911", info("aaaa"), 1, true, "")
	snap := tbl.Snapshot()
	if len(snap) != 1 || snap[0].State != Online {
		t.Fatalf("snapshot = %+v", snap)
	}
	if len(tbl.Online()) != 1 {
		t.Fatalf("online = %+v", tbl.Online())
	}
}

func TestIncompatibleWhenProtocolDiffers(t *testing.T) {
	tbl := NewTable(time.Second)
	tbl.Observe("addr", info("bbbb"), 99, false, "")
	if got := tbl.Snapshot()[0].State; got != Incompatible {
		t.Fatalf("protocol mismatch state = %s", got)
	}
}

func TestExpireMarksOffline(t *testing.T) {
	tbl := NewTable(10 * time.Millisecond)
	tbl.Observe("addr", info("cccc"), 1, true, "")
	time.Sleep(30 * time.Millisecond)
	tbl.Expire()
	if got := tbl.Snapshot()[0].State; got != Offline {
		t.Fatalf("state after timeout = %s", got)
	}
	if len(tbl.Online()) != 0 {
		t.Fatal("expired peer must not be considered online")
	}
}

func TestFailureAndSynchronizingStates(t *testing.T) {
	tbl := NewTable(time.Minute)
	tbl.Observe("addr", info("dddd"), 1, true, "")
	tbl.SetState("dddd", Synchronizing)
	if tbl.Snapshot()[0].State != Synchronizing {
		t.Fatal("synchronizing state not applied")
	}
	tbl.ObserveFailure("addr", "dddd")
	if tbl.Snapshot()[0].State != Offline {
		t.Fatal("failed probe must mark offline")
	}
	// A successful probe returns it to a normal state.
	tbl.Observe("addr", info("dddd"), 1, true, "")
	if tbl.Snapshot()[0].State != Online {
		t.Fatalf("state after recovery = %s", tbl.Snapshot()[0].State)
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	tbl := NewTable(time.Minute)
	tbl.Observe("addr", info("eeee"), 1, true, "")
	snap := tbl.Snapshot()
	snap[0].State = Incompatible
	if tbl.Snapshot()[0].State == Incompatible {
		t.Fatal("snapshot entries must not alias table state")
	}
}

// A peer behind a one-directional firewall answers discovery probes yet cannot
// be dialed for transfer, so a successful probe must not hide that outage.
func TestTransportFailuresStayVisible(t *testing.T) {
	tbl := NewTable(time.Second)
	tbl.Observe("10.0.0.1:8911", info("dddd"), 1, true, "")
	if !tbl.Snapshot()[0].Reachable {
		t.Fatal("a peer seen for the first time should not look broken")
	}
	if n := tbl.MarkTransportFailure("dddd", "dial tcp 10.0.0.1:8909: connect: connection refused"); n != 1 {
		t.Fatalf("failure count = %d", n)
	}
	e := tbl.Snapshot()[0]
	if e.Reachable || e.Failures != 1 || e.LastError == "" {
		t.Fatalf("after failure = %+v", e)
	}
	tbl.Observe("10.0.0.1:8911", info("dddd"), 1, true, "")
	if e = tbl.Snapshot()[0]; e.Reachable || e.Failures != 1 || e.LastError == "" {
		t.Fatalf("probe erased the transport failure: %+v", e)
	}
	tbl.ClearTransportFailures("dddd")
	if e = tbl.Snapshot()[0]; !e.Reachable || e.Failures != 0 || e.LastError != "" {
		t.Fatalf("after a good round = %+v", e)
	}
}
