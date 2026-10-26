package main

import (
	"strings"
	"testing"
)

// `list` fetches with force, so it works with a service's sync off; `show` reads only
// the local graph, which never received those resources. A miss has to say why.

const (
	secretARN = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:prod/db-AbCdEf"
	ssmARN    = "arn:aws:ssm:eu-west-1:123456789012:parameter/prod/db"
	logARN    = "arn:aws:logs:eu-west-1:123456789012:log-group:/prod/app:*"
)

func showHome(t *testing.T, bin string, syncOn ...string) string {
	t.Helper()
	home := graphHome(t, map[string]string{"eu-west-1/infra": exitGoodInfraNT})
	for _, service := range syncOn {
		if _, stderr, exit := runLocal(t, bin, home, "config", "set", "aws."+service+".sync", "true"); exit != 0 {
			t.Fatalf("config set aws.%s.sync true: exit %d\n%s", service, exit, stderr)
		}
	}
	return home
}

func TestShowMissHintsAtSyncOff(t *testing.T) {
	bin := build(t)

	t.Run("a typed miss names the key of its service", func(t *testing.T) {
		home := showHome(t, bin)

		_, stderr, exit := runLocal(t, bin, home, "show", secretARN, "--local")
		if exit != 1 {
			t.Errorf("exit = %d, want 1", exit)
		}
		if !strings.Contains(stderr, "not found") {
			t.Errorf("the not-found message should stay, got:\n%s", stderr)
		}
		for _, want := range []string{
			"aws.secretsmanager.sync is off",
			"`awless-ro config set aws.secretsmanager.sync true`",
			"`awless-ro sync`",
			"`awless-ro list secrets`",
		} {
			if !strings.Contains(stderr, want) {
				t.Errorf("stderr missing %q, got:\n%s", want, stderr)
			}
		}
		if strings.Contains(stderr, "aws.ssm.sync") {
			t.Errorf("a typed miss should name only its own service, got:\n%s", stderr)
		}
	})

	t.Run("a log group ARN names the monitoring key", func(t *testing.T) {
		home := showHome(t, bin)

		_, stderr, _ := runLocal(t, bin, home, "show", logARN, "--local")
		if !strings.Contains(stderr, "aws.monitoring.sync is off") {
			t.Errorf("want the monitoring key, got:\n%s", stderr)
		}
		if strings.Contains(stderr, "aws.kms.sync") {
			t.Errorf("a typed miss should name only its own service, got:\n%s", stderr)
		}
	})

	t.Run("a typed miss with its service synced has no hint", func(t *testing.T) {
		home := showHome(t, bin, "ssm")

		_, stderr, exit := runLocal(t, bin, home, "show", ssmARN, "--local")
		if exit != 1 {
			t.Errorf("exit = %d, want 1", exit)
		}
		if !strings.Contains(stderr, "not found") {
			t.Errorf("the not-found message should stay, got:\n%s", stderr)
		}
		if strings.Contains(stderr, ".sync") || strings.Contains(stderr, "config set") {
			t.Errorf("no hint expected when the service syncs, got:\n%s", stderr)
		}
	})

	t.Run("an untyped miss lists every service that is off, sorted", func(t *testing.T) {
		// ServiceNames lists monitoring before kms, so an unsorted answer differs.
		home := showHome(t, bin, "secretsmanager", "ssm")

		_, stderr, exit := runLocal(t, bin, home, "show", "no-such-thing", "--local")
		if exit != 1 {
			t.Errorf("exit = %d, want 1", exit)
		}
		if n := strings.Count(stderr, ".sync"); n < 2 {
			t.Fatalf("expected the hint, got:\n%s", stderr)
		}
		if !strings.Contains(stderr, "aws.kms.sync, aws.monitoring.sync are off") {
			t.Errorf("want both keys, sorted, in one line, got:\n%s", stderr)
		}
		if strings.Contains(stderr, "aws.ssm.sync") || strings.Contains(stderr, "aws.secretsmanager.sync") {
			t.Errorf("services that sync must not be named, got:\n%s", stderr)
		}
		if got := strings.Count(stderr, "`awless-ro sync`"); got != 1 {
			t.Errorf("want one hint line, got %d:\n%s", got, stderr)
		}
	})

	t.Run("an untyped miss with every service synced has no hint", func(t *testing.T) {
		home := showHome(t, bin, "kms", "monitoring", "secretsmanager", "ssm")

		_, stderr, _ := runLocal(t, bin, home, "show", "no-such-thing", "--local")
		if !strings.Contains(stderr, "not found") {
			t.Errorf("the not-found message should stay, got:\n%s", stderr)
		}
		if strings.Contains(stderr, ".sync") || strings.Contains(stderr, "config set") {
			t.Errorf("no hint expected, got:\n%s", stderr)
		}
	})

	t.Run("a found reference is untouched", func(t *testing.T) {
		home := showHome(t, bin)

		stdout, stderr, exit := runLocal(t, bin, home, "show", "i-1", "--local")
		if exit != 0 {
			t.Errorf("exit = %d, want 0\n%s", exit, stderr)
		}
		if !strings.Contains(stdout, "i-1") {
			t.Errorf("stdout should show the instance, got:\n%s", stdout)
		}
		if strings.Contains(stdout+stderr, ".sync") {
			t.Errorf("a found reference must not hint, got:\n%s\n%s", stdout, stderr)
		}
	})
}
