//go:build windows

package main

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32           = syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleMode = kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode = kernel32.NewProc("SetConsoleMode")
)

const enableVirtualTerminalProcessing = 0x0004

// enableVT turns on escape-sequence support in the Windows console (colours,
// cursor movement). Windows Terminal has it on already; the classic console
// needs asking. It reports false when the console can't do it.
func enableVT(f *os.File) bool {
	h := f.Fd()
	var mode uint32
	if r, _, _ := procGetConsoleMode.Call(h, uintptr(unsafe.Pointer(&mode))); r == 0 {
		return false
	}
	if mode&enableVirtualTerminalProcessing != 0 {
		return true
	}
	r, _, _ := procSetConsoleMode.Call(h, uintptr(mode|enableVirtualTerminalProcessing))
	return r != 0
}
