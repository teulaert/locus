//go:build !unix

package main

// init --system is refused before ownership matters on these platforms.
func (s *serviceUser) owns(string) bool { return true }
