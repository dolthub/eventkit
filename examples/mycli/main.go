package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/dolthub/eventkit"
	posthogtx "github.com/dolthub/eventkit/transport/posthog"
)

const (
	appName        = "mycli"
	appVersion     = "0.1.0"
	sendMetricsCmd = "send-metrics"
	envAPIKey      = "MYCLI_POSTHOG_API_KEY"
	envDisable     = "MYCLI_DISABLE_METRICS"
	envSkipSpawn   = "MYCLI_DISABLE_EVENT_FLUSH"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	dataDir := mustDataDir()
	disabled := os.Getenv(envDisable) == "1"

	switch os.Args[1] {
	case sendMetricsCmd:
		os.Exit(runSendMetrics(dataDir, disabled))
	case "foo":
		os.Exit(runInstrumented(dataDir, disabled, doFoo))
	case "bar":
		os.Exit(runInstrumented(dataDir, disabled, doBar))
	case "baz":
		os.Exit(runInstrumented(dataDir, disabled, doBaz))
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mycli {foo|bar|baz}")
	fmt.Fprintln(os.Stderr, "       mycli send-metrics   (hidden; spawned by the parent process)")
}

func mustDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot resolve home dir: %v\n", err)
		os.Exit(1)
	}
	return filepath.Join(home, "."+appName, "eventsData")
}

func runInstrumented(dataDir string, disabled bool, work func() int) int {
	var emitter eventkit.Emitter = eventkit.NullEmitter{}
	if !disabled {
		fe, err := eventkit.NewFileEmitter(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FileEmitter: %v\n", err)
			return 1
		}
		emitter = fe
	}

	c := eventkit.NewCollector(emitter,
		eventkit.WithDistinctID(eventkit.MachineID(appName)),
		eventkit.WithAppName(appName),
		eventkit.WithAppVersion(appVersion),
		eventkit.WithDisabled(func() bool { return disabled }),
	)
	eventkit.SetGlobal(c)

	code := work()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "collector close: %v\n", err)
	}
	maybeSpawnFlusher(disabled)
	return code
}

func doFoo() int {
	evt := eventkit.NewEvent("command.foo")
	defer eventkit.Global().CloseEventAndAdd(evt)

	timer := eventkit.NewTimer("foo_duration")
	defer func() {
		timer.Stop()
		evt.AddMetric(timer)
	}()

	evt.SetAttribute("flag", "--quick")
	time.Sleep(15 * time.Millisecond)

	fmt.Println("foo: done")
	return 0
}

func doBar() int {
	evt := eventkit.NewEvent("command.bar")
	defer eventkit.Global().CloseEventAndAdd(evt)

	timer := eventkit.NewTimer("bar_duration")
	defer func() {
		timer.Stop()
		evt.AddMetric(timer)
	}()

	evt.SetAttribute("remote_scheme", "https")
	evt.SetAttribute("variant", "default")

	bytes := eventkit.NewCounter("bytes_transferred")
	rows := eventkit.NewCounter("rows_processed")
	for i := 0; i < 5; i++ {
		time.Sleep(10 * time.Millisecond)
		bytes.Add(2048)
		rows.Add(100)
	}
	evt.AddMetric(bytes)
	evt.AddMetric(rows)

	fmt.Println("bar: done")
	return 0
}

func doBaz() int {
	evt := eventkit.NewEvent("command.baz")
	defer eventkit.Global().CloseEventAndAdd(evt)

	timer := eventkit.NewTimer("baz_duration")
	defer func() {
		timer.Stop()
		evt.AddMetric(timer)
	}()

	evt.SetAttribute("mode", "validate")

	retries := eventkit.NewCounter("retries")
	for i := 0; i < 3; i++ {
		time.Sleep(8 * time.Millisecond)
		retries.Inc()
	}
	evt.AddMetric(retries)

	evt.SetAttribute("status", "error")
	evt.SetAttribute("error_kind", "validation_failed")
	fmt.Fprintln(os.Stderr, "baz: validation failed")
	return 1
}

func maybeSpawnFlusher(disabled bool) {
	if disabled || os.Getenv(envSkipSpawn) == "1" {
		return
	}
	self, err := os.Executable()
	if err != nil {
		return
	}
	cmd := exec.Command(self, sendMetricsCmd)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	_ = cmd.Start()
	_ = cmd.Process.Release()
}

func runSendMetrics(dataDir string, disabled bool) int {
	if disabled {
		return 0
	}

	apiKey := os.Getenv(envAPIKey)
	if apiKey == "" {
		return 0
	}

	ph, err := posthogtx.New(posthogtx.Config{APIKey: apiKey})
	if err != nil {
		fmt.Fprintf(os.Stderr, "posthog: %v\n", err)
		return 1
	}

	flusher := eventkit.NewFileFlusher(dataDir, ph)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := flusher.Flush(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		return 1
	}
	return 0
}
