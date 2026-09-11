package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBrokerNewInferenceRequiresCurrentLease(t *testing.T) {
	for _, kind := range []string{"superseded-generation", "superseded-expiry", "current"} {
		t.Run(kind, func(t *testing.T) {
			f := newBrokerFixture(t, "success")
			cfg := brokerTestConfig(t, f)
			_, server := startBrokerTest(t, cfg)
			c := brokerTestContext(cfg)
			first, body := brokerTestControlContext(brokerRenewPath, c)
			first.OperationID = "original-lease"
			status, _, err := brokerTestHTTP(context.Background(), server.URL, brokerRenewPath, first, body)
			if err != nil || status != http.StatusOK {
				t.Fatal("could not establish original lease")
			}
			renewed := first
			renewed.OperationID = "renewed-lease"
			renewed.LeaseGeneration++
			renewed.LeaseExpiresAt = time.Now().Add(20 * time.Second).UTC().Format(time.RFC3339Nano)
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerRenewPath, renewed, body)
			if err != nil || status != http.StatusOK {
				t.Fatal("exact renewal did not acknowledge")
			}
			if kind != "superseded-generation" {
				c.LeaseGeneration = renewed.LeaseGeneration
			}
			if kind != "superseded-expiry" {
				c.LeaseExpiresAt = renewed.LeaseExpiresAt
			}
			c.OperationID = "fresh-inference"
			before, err := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
			if err != nil {
				t.Fatal("could not read original ownership")
			}
			status, _, err = brokerTestHTTP(context.Background(), server.URL, brokerResponsesPath, c, brokerTestBody(""))
			creates, inferences, _, _ := f.counts()
			if err != nil {
				t.Fatal("broker response unreadable")
			}
			if kind != "current" {
				after, readErr := os.ReadFile(filepath.Join(cfg.stateDir, "state.json"))
				if status != http.StatusConflict || creates != 0 || inferences != 0 || readErr != nil || !bytes.Equal(before, after) {
					t.Fatalf("superseded lease admitted new work: status=%d creates=%d inferences=%d", status, creates, inferences)
				}
			} else if status != http.StatusOK || creates != 1 || inferences != 1 {
				t.Fatalf("current lease rejected: status=%d creates=%d inferences=%d", status, creates, inferences)
			}
		})
	}
}
