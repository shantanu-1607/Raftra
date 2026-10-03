package main

import (
	"strings"
	"testing"
)

func TestVisibleLenIgnoresColour(t *testing.T) {
	useColor = true
	defer func() { useColor = false }()
	s := orange("leader") + " " + dim("term 4")
	if got := visibleLen(s); got != len("leader term 4") {
		t.Fatalf("visibleLen = %d, want %d", got, len("leader term 4"))
	}
	if got := visibleLen(padRight(s, 20)); got != 20 {
		t.Fatalf("padRight width = %d, want 20", got)
	}
}

func TestResultLayout(t *testing.T) {
	useColor = false
	got := result(cTeal, "city = mumbai", "Committed.", "Second line.")
	want := glyphDot + " city = mumbai\n  " + glyphTree + "  Committed.\n     Second line.\n"
	if got != want {
		t.Fatalf("result =\n%q\nwant\n%q", got, want)
	}
}

func TestBoxLinesLineUp(t *testing.T) {
	useColor = true
	defer func() { useColor = false }()
	lines := strings.Split(strings.TrimSuffix(box(30, bold("Welcome"), "", dim("cluster")+"  a.example"), "\n"), "\n")
	for _, l := range lines {
		if visibleLen(l) != 34 {
			t.Fatalf("box line %q is %d wide, want 34", l, visibleLen(l))
		}
	}
}

func TestLeaderOfPrefersHighestTerm(t *testing.T) {
	nodes := []nodeStatus{
		{host: "n1", st: &StatusResponse{IsLeader: true, Term: 3, LeaderID: "node1"}},
		{host: "n2", st: &StatusResponse{IsLeader: true, Term: 5, LeaderID: "node2"}},
		{host: "n3"},
	}
	if l := leaderOf(nodes); l == nil || l.host != "n2" {
		t.Fatalf("leaderOf picked %+v, want n2", l)
	}
	if answered(nodes) != 2 {
		t.Fatalf("answered = %d, want 2", answered(nodes))
	}
	if leaderOf(nodes[2:]) != nil {
		t.Fatal("leaderOf found a leader among nodes that didn't answer")
	}
}

func TestStatusLines(t *testing.T) {
	useColor = false
	nodes := []nodeStatus{
		{host: "b.example", st: &StatusResponse{Role: "Follower", Term: 4, CommitIndex: 9}},
		{host: "a.example", st: &StatusResponse{Role: "Leader", IsLeader: true, Term: 4, CommitIndex: 9}},
		{host: "c.example"},
	}
	got := statusLines(nodes)
	want := []string{
		"a.example  leader     term 4    commit 9",
		"b.example  follower   term 4    commit 9",
		"c.example  no answer",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("statusLines =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestBannerLineHasMark(t *testing.T) {
	useColor = false
	if got := bannerLine(0); !strings.HasSuffix(got, "╗   ██      ") {
		t.Fatalf("bannerLine(0) = %q, want the leader bar", got)
	}
	if got := bannerLine(3); !strings.HasSuffix(got, "██ ██ ██") {
		t.Fatalf("bannerLine(3) = %q, want leader and follower bars", got)
	}
	if got := bannerLine(len(banner) - 1); strings.HasSuffix(got, "██") {
		t.Fatalf("last banner line should have no bars: %q", got)
	}
}
