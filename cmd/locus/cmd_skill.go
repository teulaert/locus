package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/teulaert/locus/internal/agent"
)

// cmdSkill prints the agent-facing documentation embedded in the binary, so
// that discovering the interface never requires an extra download.
func cmdSkill(g *globalOptions, args []string) int {
	fs := flag.NewFlagSet("skill", flag.ContinueOnError)
	section := fs.String("section", "", "print only one section, e.g. \"exit codes\"")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *section != "" {
		body := agent.Section(*section)
		if body == "" {
			fmt.Fprintf(os.Stderr, "locus: no such section %q\n", *section)
			return exitUsage
		}
		if g.json {
			printJSON(map[string]any{"section": *section, "body": body})
		} else {
			fmt.Println(body)
		}
		return exitOK
	}
	if g.json {
		printJSON(map[string]any{"skill": agent.Skill()})
	} else {
		fmt.Print(agent.Skill())
	}
	return exitOK
}
