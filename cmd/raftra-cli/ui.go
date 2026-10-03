package main

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"
)

// ============================================================
// Theme: the landing page's "flight recorder" palette
// ============================================================

var (
	cOrange = rgbFG(255, 122, 26) // the leader, the brand
	cBone   = rgbFG(233, 228, 216)
	cDim    = rgbFG(142, 151, 157)
	cRule   = rgbFG(83, 94, 102)
	cRed    = rgbFG(240, 71, 59)  // failures, dead nodes
	cTeal   = rgbFG(85, 207, 171) // committed, success
	cAmber  = rgbFG(232, 197, 71) // warnings, elections
)

const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	clearLine  = "\033[2K"
	hideCursor = "\033[?25l"
	showCursor = "\033[?25h"
)

// Terminal capabilities, set once by setupTerminal.
var (
	useColor  bool // 24-bit colour (off for pipes, NO_COLOR and TERM=dumb)
	stdoutTTY bool // spinner, animation and cursor movement are allowed
	stdinTTY  bool // input is typed by a person
)

// Glyphs. Windows console fonts often lack the fancier ones.
var (
	glyphDot   = "●"
	glyphTree  = "⎿"
	glyphStar  = "✻"
	spinGlyphs = []string{"·", "✢", "✳", "✶", "✻", "✽", "✻", "✶", "✳", "✢"}
)

func init() {
	if runtime.GOOS == "windows" {
		glyphTree = "└"
		glyphStar = "*"
		spinGlyphs = []string{"·", "•", "●", "•"}
	}
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func setupTerminal() {
	stdinTTY = isTerminal(os.Stdin)
	stdoutTTY = isTerminal(os.Stdout) && os.Getenv("TERM") != "dumb"
	if stdoutTTY && !enableVT(os.Stdout) {
		stdoutTTY = false // a console without escape-sequence support gets plain output
	}
	useColor = stdoutTTY && os.Getenv("NO_COLOR") == ""
}

func rgbFG(r, g, b int) string { return fmt.Sprintf("\033[38;2;%d;%d;%dm", r, g, b) }

func paint(color, s string) string {
	if !useColor || s == "" {
		return s
	}
	return color + s + ansiReset
}

func orange(s string) string { return paint(cOrange, s) }
func bone(s string) string   { return paint(cBone, s) }
func dim(s string) string    { return paint(cDim, s) }
func rule(s string) string   { return paint(cRule, s) }
func bold(s string) string   { return paint(ansiBold, s) }

var ansiRE = regexp.MustCompile("\x1b\\[[0-9;?]*[A-Za-z]")

// visibleLen is the on-screen width of s, ignoring colour codes.
func visibleLen(s string) int { return utf8.RuneCountInString(ansiRE.ReplaceAllString(s, "")) }

func padRight(s string, w int) string {
	if n := visibleLen(s); n < w {
		return s + strings.Repeat(" ", w-n)
	}
	return s
}

// ============================================================
// Output blocks, laid out like Claude Code's: a coloured bullet with
// a one-line summary, and details hanging off a "⎿" underneath.
// ============================================================

func result(color, summary string, details ...string) string {
	var b strings.Builder
	b.WriteString(paint(color, glyphDot) + " " + summary + "\n")
	for i, d := range details {
		if i == 0 {
			b.WriteString("  " + dim(glyphTree) + "  " + d + "\n")
		} else {
			b.WriteString("     " + d + "\n")
		}
	}
	return b.String()
}

func printResult(color, summary string, details ...string) {
	fmt.Print(result(color, summary, details...))
}

func printFail(summary string, details ...string) {
	fmt.Fprint(os.Stderr, result(cRed, summary, details...))
}

// box draws a rounded box with inner width w around lines.
func box(w int, lines ...string) string {
	var b strings.Builder
	b.WriteString(rule("╭"+strings.Repeat("─", w+2)+"╮") + "\n")
	for _, l := range lines {
		b.WriteString(rule("│") + " " + padRight(l, w) + " " + rule("│") + "\n")
	}
	b.WriteString(rule("╰"+strings.Repeat("─", w+2)+"╯") + "\n")
	return b.String()
}

// spread puts left and right at the two ends of a line w wide.
func spread(left, right string, w int) string {
	gap := max(w-visibleLen(left)-visibleLen(right), 1)
	return left + strings.Repeat(" ", gap) + right
}

// boxWidth is the inner width for boxes: 60 columns, less on narrow terminals.
func boxWidth() int {
	w := 60
	var cols int
	if _, err := fmt.Sscan(os.Getenv("COLUMNS"), &cols); err == nil && cols > 20 && cols-4 < w {
		w = cols - 4
	}
	return w
}

// ============================================================
// Banner: the wordmark plus the three-bar mark (leader orange, two followers)
// ============================================================

var banner = []string{
	`██████╗   █████╗  ███████╗ ████████╗ ██████╗   █████╗ `,
	`██╔══██╗ ██╔══██╗ ██╔════╝ ╚══██╔══╝ ██╔══██╗ ██╔══██╗`,
	`██████╔╝ ███████║ █████╗      ██║    ██████╔╝ ███████║`,
	`██╔══██╗ ██╔══██║ ██╔══╝      ██║    ██╔══██╗ ██╔══██║`,
	`██║  ██║ ██║  ██║ ██║         ██║    ██║  ██║ ██║  ██║`,
	`╚═╝  ╚═╝ ╚═╝  ╚═╝ ╚═╝         ╚═╝    ╚═╝  ╚═╝ ╚═╝  ╚═╝`,
}

func bannerLine(i int) string {
	var b strings.Builder
	b.WriteString("  ")
	// Solid blocks in bone, the box-drawing "shadow" in the rule colour.
	var run []rune
	solid := false
	flush := func() {
		if len(run) == 0 {
			return
		}
		if solid {
			b.WriteString(bone(string(run)))
		} else {
			b.WriteString(rule(string(run)))
		}
		run = run[:0]
	}
	for _, r := range banner[i] {
		isSolid := r == '█'
		if isSolid != solid && r != ' ' {
			flush()
			solid = isSolid
		}
		run = append(run, r)
	}
	flush()
	follower := "  "
	if i >= 2 {
		follower = "██"
	}
	if i < len(banner)-1 {
		b.WriteString("  " + orange("██") + " " + bone(follower) + " " + bone(follower))
	}
	return b.String()
}

func printBanner(animate bool) {
	if animate {
		fmt.Print(hideCursor)
		defer fmt.Print(showCursor)
	}
	for i := range banner {
		fmt.Println(bannerLine(i))
		if animate {
			time.Sleep(35 * time.Millisecond)
		}
	}
}

// ============================================================
// Spinner, shown while a request is in flight
// ============================================================

type spinner struct {
	done     chan struct{}
	finished chan struct{}
}

func startSpinner(label string) *spinner {
	s := &spinner{done: make(chan struct{}), finished: make(chan struct{})}
	if !stdoutTTY {
		close(s.finished)
		return s
	}
	go func() {
		defer close(s.finished)
		tick := time.NewTicker(110 * time.Millisecond)
		defer tick.Stop()
		for i := 0; ; i++ {
			fmt.Printf("\r%s %s", orange(spinGlyphs[i%len(spinGlyphs)]), dim(label))
			select {
			case <-s.done:
				fmt.Print("\r" + clearLine)
				return
			case <-tick.C:
			}
		}
	}()
	return s
}

func (s *spinner) stop() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	<-s.finished
}
