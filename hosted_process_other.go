//go:build !linux

package main

import "context"

func launchHostedSupervisor(context.Context, map[string]string) (<-chan error, error) {
	return nil, errHostedInvalid
}
