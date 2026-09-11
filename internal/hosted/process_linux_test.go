//go:build linux

package hosted

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestHostedSupervisorProcessShutdownScopes(t *testing.T) {
	for _, mode := range []string{"graceful", "force-group", "exit-group", "force-detached"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			command := hostedProcessTestCommand(mode, directory)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done, err := startHostedSupervisorProcess(ctx, command, 250*time.Millisecond, 2*time.Second)
			if err != nil {
				t.Fatal("could not launch the isolated supervisor fixture")
			}
			t.Cleanup(func() {
				cancel()
				_ = command.Process.Kill()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("supervisor fixture did not finish cleanup")
				}
			})
			hostedProcessTestFile(t, filepath.Join(directory, "ready"))
			childFD := -1
			if mode != "graceful" {
				childPID, err := strconv.Atoi(string(hostedProcessTestFile(t, filepath.Join(directory, "child-pid"))))
				if err != nil || childPID <= 0 {
					t.Fatal("fixture did not identify its child")
				}
				childFD, err = unix.PidfdOpen(childPID, 0)
				if err != nil {
					t.Fatal("could not retain the exact fixture child")
				}
				t.Cleanup(func() {
					_ = unix.PidfdSendSignal(childFD, unix.SIGKILL, nil, 0)
					hostedProcessTestExited(t, childFD, true)
					_ = unix.Close(childFD)
				})
			}
			cancel()
			select {
			case err := <-done:
				forced := mode == "force-group" || mode == "force-detached"
				if forced && !errors.Is(err, errHostedInvalid) || !forced && err != nil {
					t.Fatal("supervisor exit was classified incorrectly")
				}
				status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
				if !ok || forced && (!status.Signaled() || status.Signal() != syscall.SIGKILL) || !forced && status.ExitStatus() != 0 {
					t.Fatal("supervisor kernel exit did not match the requested shutdown")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("supervisor shutdown exceeded the fixture bound")
			}
			if mode == "graceful" {
				hostedProcessTestFile(t, filepath.Join(directory, "cleaned"))
			} else {
				// An independent ACP process group is intentionally outside
				// this fallback. Only Orka's UID owner can prove its cleanup.
				hostedProcessTestExited(t, childFD, mode != "force-detached")
			}
		})
	}
}

func TestHostedSupervisorProcessRejectsCancelledLaunch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	command := hostedProcessTestCommand("graceful", t.TempDir())
	if _, err := startHostedSupervisorProcess(ctx, command, time.Second, time.Second); err == nil || command.Process != nil {
		t.Fatal("cancelled hosted lifetime launched a process")
	}
	for _, pid := range []int{0, -1} {
		if signalHostedSupervisorGroup(pid, syscall.SIGKILL) == nil {
			t.Fatal("invalid supervisor identity was accepted for group signaling")
		}
	}
}

func hostedProcessTestCommand(mode, directory string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestHostedProcessHelper$")
	// A race-instrumented fixture must not add the detector's default one-second
	// exit sleep to the deliberately short graceful-shutdown test window.
	command.Env = []string{"FOUNDRY_HOSTED_PROCESS_HELPER=" + mode, "FOUNDRY_HOSTED_PROCESS_DIRECTORY=" + directory,
		"GORACE=atexit_sleep_ms=0"}
	return command
}

func hostedProcessTestFile(t *testing.T, path string) []byte {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if data, err := os.ReadFile(path); err == nil && len(data) != 0 {
			return data
		}
		select {
		case <-deadline.C:
			t.Fatal("supervisor fixture did not reach its checkpoint")
		case <-tick.C:
		}
	}
}

func hostedProcessTestExited(t *testing.T, fd int, want bool) {
	t.Helper()
	timeout := 0
	if want {
		timeout = 1000
	}
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, timeout)
	if err != nil || (n > 0 && fds[0].Revents&unix.POLLIN != 0) != want {
		t.Fatal("exact fixture child exit state did not match its process-group scope")
	}
}

func TestHostedProcessHelper(t *testing.T) {
	mode := os.Getenv("FOUNDRY_HOSTED_PROCESS_HELPER")
	if mode == "" {
		return
	}
	directory := os.Getenv("FOUNDRY_HOSTED_PROCESS_DIRECTORY")
	write := func(name, value string) {
		if os.WriteFile(filepath.Join(directory, name), []byte(value), 0o600) != nil {
			os.Exit(3)
		}
	}
	if mode == "child" {
		signal.Ignore(syscall.SIGTERM)
		write("child-pid", strconv.Itoa(os.Getpid()))
		for {
			time.Sleep(time.Hour)
		}
	}
	terminated := make(chan os.Signal, 1)
	if mode == "force-group" || mode == "force-detached" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(terminated, syscall.SIGTERM)
	}
	if mode != "graceful" {
		child := hostedProcessTestCommand("child", directory)
		if mode == "force-detached" {
			child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		}
		if child.Start() != nil {
			os.Exit(4)
		}
	}
	write("ready", "1")
	if mode == "force-group" || mode == "force-detached" {
		for {
			time.Sleep(time.Hour)
		}
	}
	<-terminated
	if mode == "graceful" {
		time.Sleep(25 * time.Millisecond)
		write("cleaned", "1")
	}
}
