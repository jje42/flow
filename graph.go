package flow

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
)

type job struct {
	Cmd                   Commander
	UUID                  uuid.UUID
	ID                    string
	Inputs                []string
	Outputs               []string
	Stdout                string
	doneFile              string
	Dependencies          []*job
	hasCompleted          bool
	completedSuccessfully bool
	BatchCommand          string
}

// Command takes the original command line and allows adding pre- or post-
// commands.
func (j job) Command() string {
	return fmt.Sprintf(`set -o errexit
set -o pipefail
set -o verbose
env | sort
%s`, j.Cmd.Command())
}

func (j job) isRunnable() bool {
	if j.hasCompleted {
		return false
	}
	for _, d := range j.Dependencies {
		if !d.hasCompleted {
			return false
		}
		// If any dependency failed, this job is not runnable.
		if !d.completedSuccessfully {
			return false
		}
	}
	return true
}

type graph struct {
	jobs      []*job
	pending   []*job
	running   []*job
	completed []*job
	failed    []*job
}

func newGraph(cmds []Commander) (graph, error) {
	g := graph{}
	for _, cmd := range cmds {
		job := &job{
			Cmd:     cmd,
			UUID:    uuid.New(),
			Inputs:  cmdInputs(cmd),
			Outputs: cmdOutputs(cmd),
		}
		// What if the job has no outputs? Is this an error, if so we should
		// check for this.
		if len(job.Outputs) == 0 {
			log.Fatal("Job has no defined outputs: " + job.Cmd.AnalysisName())
		}
		job.Stdout = fmt.Sprintf("%s.out", job.Outputs[0])
		dir, file := filepath.Split(job.Outputs[0])
		job.doneFile = filepath.Join(dir, fmt.Sprintf(".%s.done", file))
		job.doneFile = filepath.Join(
			v.GetString("flowdir"),
			"done",
			strings.TrimSuffix(job.Stdout, ".out")+".done",
		)
		g.jobs = append(g.jobs, job)
	}
	for _, j := range g.jobs {
		j.Dependencies = dependenciesFor(j, g.jobs)
		g.pending = append(g.pending, j)
	}

	if v.GetBool("start_from_scratch") {
		for _, j := range g.jobs {
			err := os.Remove(j.doneFile)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return g, fmt.Errorf("unable to remove done file: %s: %v", j.doneFile, err)
			}
		}
	}
	// If the done file exists for any pending job, mark it as complete and move
	// it to the completed list.
	pendingList := make([]*job, len(g.pending))
	copy(pendingList, g.pending)
	for _, p := range pendingList {
		ok, err := fileExists(p.doneFile)
		if err != nil {
			return g, fmt.Errorf("unable to determine if file exists: %s: %v", p.doneFile, err)
		}
		if ok {
			p.hasCompleted = true
			p.completedSuccessfully = true
			idx, err := jobIndex(p, g.pending)
			if err != nil {
				return g, fmt.Errorf("unable to find job index: %s: %v", p.UUID, err)
			}
			g.completed = append(g.completed, p)
			g.pending = append(g.pending[:idx], g.pending[idx+1:]...)
		}
	}
	return g, nil
}

func dependenciesFor(j *job, allJobs []*job) []*job {
	ds := []*job{}
	for _, otherJob := range allJobs {
		if j.UUID != otherJob.UUID {
			if hasIntersection(j.Inputs, otherJob.Outputs) {
				ds = append(ds, otherJob)
			}
		}
	}
	return ds
}

// What happens if a job fails? How do we stop subsequent jobs being run while
// still exiting the loop eventually.
func (g graph) Process() error {
	var runner Runner
	var err error
	switch runnerStr := v.GetString("job_runner"); runnerStr {
	case "pbs":
		runner, err = NewPBSRunner()
		if err != nil {
			return err
		}
	case "slurm":
		runner, err = NewSlurmRunner()
		if err != nil {
			return err
		}
	case "local":
		runner = NewLocalRunner()
	case "dummy":
		runner = DummyRunner{}
	default:
		log.Fatalf("Unknown runner requested: %s", runnerStr)
	}
	// Ensure that however we leave this function any running jobs are
	// terminated.
	defer killRunningJobs(g, runner)

	// "yyyy-MM-DD_HHmmss"
	t := time.Now()
	timestamp := fmt.Sprintf(
		"%d-%02d-%02d_%02d%02d%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(),
	)
	jobReportfile := fmt.Sprintf("jobreport_%s.csv", timestamp)
	w, err := os.Create(jobReportfile)
	if err != nil {
		return fmt.Errorf("unable to create job report file: %v", err)
	}
	defer w.Close()
	report, err := NewJobreport(w)
	if err != nil {
		return fmt.Errorf("unable to create job report: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	quit := make(chan bool)
	allStopped := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// Set a goroutine to gracefully shutdown running jobs if SIGINT or SIGTERM
	// recieved.
	go func() {
		<-sigs
		quit <- true
		<-allStopped // is this actually necessary? Will the send to quit block until received?
		log.Printf("Shutting down jobs")
		err := killRunningJobs(g, runner)
		if err != nil {
			log.Printf("Signal handler unable to kill all jobs: %s", err)
		}
		os.Exit(1)
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	errs := make(chan error, 1)

	go func() {
		defer wg.Done()
		defer close(errs)
		_, err = g.submitPending(runner)
		if err != nil {
			errs <- fmt.Errorf("failed to submit jobs: %v", err)
			return
		}
		log.Printf("%d pending, %d running, %d failed, %d done", len(g.pending), len(g.running), len(g.failed), len(g.completed))

		for {
			select {
			case <-quit:
				allStopped <- true
				time.Sleep(1 * time.Hour)
			default:
				nCompleted, err := g.checkCompleted(runner, report)
				if err != nil {
					errs <- fmt.Errorf("failed to check running jobs: %v", err)
					return
				}
				nSubmitted, err := g.submitPending(runner)
				if err != nil {
					errs <- fmt.Errorf("failed to submit jobs: %v", err)
					return
				}
				if nSubmitted == 0 && len(g.pending) != 0 && len(g.running) == 0 {
					// If no jobs were submitted but there are pending jobs
					// and no running jobs it must mean jobs cannot run
					// because of previous failures.
					log.Printf("There are no more jobs that can be run.")
					return
				}
				if nCompleted > 0 || nSubmitted > 0 {
					log.Printf("%d pending, %d running, %d failed, %d done", len(g.pending), len(g.running), len(g.failed), len(g.completed))
				}
				if len(g.pending) == 0 && len(g.running) == 0 {
					log.Printf("There are no more jobs to run")
					return
				}
				time.Sleep(60 * time.Second)
			}
		}
	}()

	wg.Wait()
	signal.Reset()
	for err := range errs {
		if err != nil {
			return err
		}
	}

	err = report.Flush()
	if err != nil {
		return fmt.Errorf("unable to finalise job report file: %v", err)
	}
	if len(g.failed) > 0 {
		boldRed := color.New(color.Bold, color.FgRed).SprintfFunc()
		log.Printf("Workflow completed with %d %s jobs, see stdout for details", len(g.failed), boldRed("FAILED"))
		for _, job := range g.failed {
			log.Printf("%s: %s, output: %s", boldRed("FAILED"), job.UUID, job.Stdout)
		}
	} else {
		greenBold := color.New(color.Bold, color.FgGreen).SprintfFunc()
		log.Printf("Workflow completed %s", greenBold("SUCCESSFULLY"))
	}
	log.Printf("Completed with %d completed and %d failed (running = %d)", len(g.completed), len(g.failed), len(g.running))
	if len(g.failed) > 0 {
		return errors.New("flow workflow completed with failures")
	}
	return nil
}

// Attempt to kill all running jobs. Returns an error if it fails to kill any of
// the jobs.
func killRunningJobs(g graph, r Runner) error {
	errs := []error{}
	for _, job := range g.running {
		err := r.Kill(job)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	} else {
		return fmt.Errorf("failed to kill %d jobs", len(errs))
	}
}

func (g *graph) submitPending(r Runner) (int, error) {
	submitted := 0
	pendingList := make([]*job, len(g.pending))
	copy(pendingList, g.pending)
	for _, pending := range pendingList {
		if pending.isRunnable() {
			ctx, err := newExecutionContext(pending)
			if err != nil {
				return submitted, fmt.Errorf("failed to create execution context for %s: %v", pending.UUID, err)
			}
			if err := r.Run(ctx); err != nil {
				return submitted, fmt.Errorf("unable to run job: %v", err)
			}
			// Display job information after it has been submitted
			// so JobID is populated.
			displayJob(pending)
			idx, err := jobIndex(pending, g.pending)
			if err != nil {
				return submitted, err
			}
			g.pending = append(g.pending[:idx], g.pending[idx+1:]...)
			g.running = append(g.running, pending)
			submitted++
		}
	}
	return submitted, nil
}

func (g *graph) checkCompleted(r Runner, report jobReport) (int, error) {
	nCompleted := 0
	runningList := make([]*job, len(g.running))
	copy(runningList, g.running)
	for _, running := range runningList {
		completed, err := r.Completed(running)
		if err != nil {
			return nCompleted, fmt.Errorf("unable to determine job state: %s: %s", running.ID, err)
		}
		if completed {
			nCompleted++
			running.hasCompleted = true
			successful, err := r.CompletedSuccessfully(running)
			if err != nil {
				return nCompleted, fmt.Errorf("unable to determine job state: %s: %s", running.ID, err)
			}
			idx, err := jobIndex(running, g.running)
			if err != nil {
				return nCompleted, err
			}
			if successful {
				running.completedSuccessfully = true
				// done files are only created on successful completion of a job.
				green := color.New(color.Bold, color.FgGreen).SprintfFunc()
				log.Printf("Job completed %s %s %s", green("SUCCESSFULLY"), running.UUID, running.ID)
				err := os.MkdirAll(filepath.Dir(running.doneFile), 0755)
				if err != nil {
					return nCompleted, fmt.Errorf("unable to create done file directory for job: %s: %s", running.ID, err)
				}
				_, err = os.Create(running.doneFile)
				if err != nil {
					return nCompleted, fmt.Errorf("unable to create done file for job: %s: %s", running.ID, err)
				}
				resources, err := r.ResourcesUsed(running)
				if err != nil {
					log.Printf("Failed to get resources for job: %v: %v", running.UUID, err)
				} else {
					err = report.Add(running, resources)
					if err != nil {
						log.Printf("Unable to update job report file: %v", err)
					}
				}
				g.completed = append(g.completed, running)
				g.running = append(g.running[:idx], g.running[idx+1:]...)
			} else {
				bold := color.New(color.Bold, color.FgRed).SprintfFunc()
				log.Printf("%s: job failed: %v, %v: stdout written to %s", bold("ERROR"), running.UUID, running.ID, running.Stdout)
				g.failed = append(g.failed, running)
				g.running = append(g.running[:idx], g.running[idx+1:]...)
			}
		}
	}
	return nCompleted, nil
}

type executionContext struct {
	job    *job
	dir    string
	script string
}

func newExecutionContext(j *job) (executionContext, error) {
	cxt := executionContext{
		job: j,
	}
	var err error
	cxt.dir, err = ioutil.TempDir(v.GetString("flowdir"), "flow")
	if err != nil {
		return executionContext{}, fmt.Errorf("failed to create temp directory: %v", err)
	}
	jobFn, err := filepath.Abs(filepath.Join(cxt.dir, "job.sh"))
	if err != nil {
		return executionContext{}, fmt.Errorf("unable to get absolute path of job.sh: %v", err)
	}
	cxt.script = jobFn
	scriptFn, err := filepath.Abs(filepath.Join(cxt.dir, "script.sh"))
	if err != nil {
		return executionContext{}, fmt.Errorf("unable to get absolute path of script.sh: %v", err)
	}

	if err := createJobFile(jobFn, scriptFn, j); err != nil {
		return executionContext{}, fmt.Errorf("unable to create job file: %v", err)
	}
	if err := createScriptFile(scriptFn, j); err != nil {
		return executionContext{}, fmt.Errorf("unable to create script file: %v", err)
	}
	return cxt, nil
}

func createJobFile(jobFile, scriptFile string, j *job) error {
	r := j.Cmd.Resources()
	shell := "/bin/bash"
	singularityBin := v.GetString("singularity_bin")
	if singularityBin == "" {
		singularityBin = "singularity"
	}
	// slurm _requires_ a shebang line
	var content strings.Builder
	content.WriteString(`#!/usr/bin/env bash
set -o verbose
env | sort
`)
	ds := []string{}
	for _, fn := range j.Outputs {
		ds = append(ds, filepath.Dir(fn))
	}
	for _, d := range unique(ds) {
		content.WriteString(fmt.Sprintf("mkdir -p %s\n", d))
		os.MkdirAll(d, 0755)
	}

	content.WriteString(fmt.Sprintf("cat %s | sed s'/^/# SCRIPT: /'\n", scriptFile))

	// Typically flowdir is inside a users home directory and this is
	// automatically bound in, but it may not be and the -C option may be
	// provided.
	if r.Container != "" {
		content.WriteString(fmt.Sprintf(
			"%s exec %s -B %s:/flowdir %s %s /flowdir/%s",
			singularityBin,
			r.SingularityExtraArgs,
			filepath.Dir(scriptFile),
			r.Container,
			shell,
			filepath.Base(scriptFile)))
	} else {
		content.WriteString(fmt.Sprintf("%s %s", shell, scriptFile))
	}

	if err := ioutil.WriteFile(jobFile, []byte(content.String()), 0664); err != nil {
		return fmt.Errorf("failed to write job script content: %v", err)
	}
	return nil
}

func unique(xs []string) []string {
	m := make(map[string]bool)
	for _, x := range xs {
		m[x] = true
	}
	ys := []string{}
	for key := range m {
		ys = append(ys, key)
	}
	return ys
}

func createScriptFile(scriptFile string, j *job) error {
	content := j.Command()
	if err := ioutil.WriteFile(scriptFile, []byte(content), 0664); err != nil {
		return fmt.Errorf("failed to write script content: %v", err)
	}
	return nil
}

// Return the value (i.e., the path) of all input fields.
func cmdTag(c Commander, t string) []string {
	inputs := []string{}
	v := reflect.ValueOf(c).Elem()
	for i := 0; i < v.NumField(); i++ {
		tag := v.Type().Field(i).Tag.Get("type")
		if tag == t {
			val := v.Field(i)
			switch val.Kind() {
			case reflect.String:
				inputs = append(inputs, val.String())
			case reflect.Slice:
				if val.Type().Elem().Name() == "string" {
					for j := 0; j < val.Len(); j++ {
						inputs = append(inputs, val.Index(j).String())
					}
				}
			}
		}
	}
	return inputs
}

func cmdInputs(c Commander) []string {
	return cmdTag(c, "input")
}

// Return the value (i.e., the path) of all output fields.
func cmdOutputs(c Commander) []string {
	return cmdTag(c, "output")
}

func hasIntersection(list1, list2 []string) bool {
	for _, i := range list1 {
		// Inputs/Outputs that are the empty string should NOT be
		// considered; otherwise every task that has an empty string
		// output will become a dependency of any task with an empty
		// string input.
		if i == "" {
			continue
		}
		for _, j := range list2 {
			if j == "" {
				continue
			}
			if i == j {
				return true
			}
		}
	}
	return false
}

type resourcesUsed struct {
	CPUPercent      int
	MemoryUsed      int
	TimeUsed        int
	CPURequested    int
	MemoryRequested int
	TimeRequested   int
	ExecHost        string
	ExitStatus      int
}

func displayJob(j *job) error {
	r := j.Cmd.Resources()
	c := []string{}
	for _, line := range strings.Split(j.Command(), "\n") {
		c = append(c, fmt.Sprintf("  %s", line))
	}
	indentedCmd := strings.Join(c, "\n")
	bold := color.New(color.Bold).SprintFunc()
	log.Printf(`Running Task:
%s: %s
%s: %s
%s: %s
%s: CPUs %d; Memory %d; Time %d:00:00
%s: %s
%s: %s
%s: %s
%s: %s
%s:
%s
%s:
%s
`,
		bold("UUID"), j.UUID,
		bold("Job ID"), j.ID,
		bold("Analysis Name"), j.Cmd.AnalysisName(),
		bold("Resources"), r.CPUs, r.Memory, r.Time,
		bold("Stdout"), j.Stdout,
		bold("DoneFile"), j.doneFile,
		bold("Container"), r.Container,
		bold("Extra Args"), r.SingularityExtraArgs,
		bold("Script"), indentedCmd,
		bold("Batch Command"), j.BatchCommand)
	return nil
}

func jobIndex(j *job, list []*job) (int, error) {
	for i, job := range list {
		if job.UUID == j.UUID {
			return i, nil
		}
	}
	return -1, fmt.Errorf("unable to find job in list, %v", j.UUID)
}

func fileExists(fn string) (bool, error) {
	if _, err := os.Stat(fn); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}
