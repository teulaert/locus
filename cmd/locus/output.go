package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func short(h string) string {
	if len(h) <= 8 {
		return h
	}
	return h[:8]
}

// printJSON writes v as indented JSON. Documented collections are kept
// non-nil by the typed responses in package app, not here.
func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
	}
}
