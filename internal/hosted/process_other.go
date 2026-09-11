//go:build !linux

package hosted

import "context"

func launchHostedSupervisor(context.Context, map[string]string) (<-chan error, error) {
	return nil, errHostedInvalid
}
