//go:build !windows

package main

import "os"

// enableVT is a no-op outside Windows: Unix terminals understand escape sequences.
func enableVT(*os.File) bool { return true }
