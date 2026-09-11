package main

import (
	"log"
	"os"

	"github.com/orka-agents/agent-runtime-foundry/internal/acp"
	"github.com/orka-agents/agent-runtime-foundry/internal/adapter"
	"github.com/orka-agents/agent-runtime-foundry/internal/broker"
	"github.com/orka-agents/agent-runtime-foundry/internal/hosted"
)

func main() {
	if handled, err := hosted.MaybeServe(os.Args[1:]); handled {
		if err != nil {
			log.Fatal("Foundry hosted lifetime unavailable; inspect the ownership ledger before replacement")
		}
		return
	}
	if handled, err := broker.MaybeServe(os.Args[1:]); handled {
		if err != nil {
			log.Fatal("Foundry lifecycle broker failed")
		}
		return
	}
	if handled, err := acp.MaybeServe(os.Args[1:], os.Stdin, os.Stdout); handled {
		if err != nil {
			log.Fatal("Foundry ACP bridge failed")
		}
		return
	}
	if err := adapter.Serve(); err != nil {
		log.Fatal(err)
	}
}
