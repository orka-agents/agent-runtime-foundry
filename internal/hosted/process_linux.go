//go:build linux

package hosted

import (
	"context"
	"os/exec"
	"sort"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func launchHostedSupervisor(ctx context.Context, environment map[string]string) (<-chan error, error) {
	command := exec.Command("/usr/local/bin/orka-acp-runtime")
	for name, value := range environment {
		command.Env = append(command.Env, name+"="+value)
	}
	sort.Strings(command.Env)
	return startHostedSupervisorProcess(ctx, command, hostedSupervisorStopGrace, hostedSupervisorKillWait)
}

func startHostedSupervisorProcess(ctx context.Context, command *exec.Cmd, grace, killWait time.Duration) (<-chan error, error) {
	if ctx.Err() != nil || grace <= 0 || killWait <= 0 {
		return nil, errHostedInvalid
	}
	command.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, Setpgid: true}
	// Nil output uses /dev/null directly, avoiding inherited copy pipes that
	// could prevent Wait from returning after the supervisor has exited.
	command.Stdout, command.Stderr = nil, nil
	if command.Start() != nil {
		return nil, errHostedInvalid
	}
	done := make(chan error, 1)
	go func() {
		done <- waitHostedSupervisorProcess(ctx, command, grace, killWait)
		close(done)
	}()
	return done, nil
}

func waitHostedSupervisorProcess(ctx context.Context, command *exec.Cmd, grace, killWait time.Duration) error {
	exited := make(chan error, 1)
	go func() {
		var info unix.Siginfo
		for {
			err := unix.Waitid(unix.P_PID, command.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
			if err != syscall.EINTR {
				exited <- err
				return
			}
		}
	}()
	// Keep the leader unreaped until every group signal is sent. Otherwise a
	// fast exit and PID reuse could redirect a later kill(-pgid) elsewhere.
	var observedErr, signalErr error
	forced := false
	select {
	case observedErr = <-exited:
	case <-ctx.Done():
		signalErr = signalHostedSupervisorGroup(command.Process.Pid, syscall.SIGTERM)
		graceTimer := time.NewTimer(grace)
		select {
		case observedErr = <-exited:
		case <-graceTimer.C:
			forced = true
			if err := signalHostedSupervisorGroup(command.Process.Pid, syscall.SIGKILL); err != nil {
				signalErr = err
			}
			killTimer := time.NewTimer(killWait)
			select {
			case observedErr = <-exited:
			case <-killTimer.C:
				// A stuck kernel task is not cleanup proof. Reap if it later
				// exits, but let the caller retire this failed lifetime now.
				go func() { <-exited; _ = command.Wait() }()
				return errHostedInvalid
			}
			killTimer.Stop()
		}
		graceTimer.Stop()
	}
	if observedErr != nil {
		_ = command.Process.Kill()
		go func() { _ = command.Wait() }()
		return errHostedInvalid
	}
	// This reaches only the supervisor's own group. ACP children deliberately
	// use separate groups and UIDs: their cleanup belongs to Orka's bounded
	// Server.Close, and remote cleanup still requires its broker receipts.
	if err := signalHostedSupervisorGroup(command.Process.Pid, syscall.SIGKILL); err != nil {
		signalErr = err
	}
	if err := command.Wait(); err != nil || signalErr != nil || forced {
		return errHostedInvalid
	}
	return nil
}

func signalHostedSupervisorGroup(pid int, signal syscall.Signal) error {
	if pid <= 0 {
		return errHostedInvalid
	}
	if err := syscall.Kill(-pid, signal); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
