package flow

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type PBSRunner struct {
	jobIDs map[uuid.UUID]string
}

func NewPBSRunner() (*PBSRunner, error) {
	r := &PBSRunner{}
	_, err := exec.LookPath("qsub")
	if err != nil {
		return r, fmt.Errorf("requested PBS runner, but unable to find qsub on PATH")
	}
	_, err = exec.LookPath("qstat")
	if err != nil {
		return r, fmt.Errorf("requested PBS runner, but unable to find qstat on PATH")
	}
	r.jobIDs = make(map[uuid.UUID]string)
	return r, nil
}

func (r *PBSRunner) Run(ctx executionContext) error {
	jobName := ctx.job.Cmd.AnalysisName()
	resources := ctx.job.Cmd.Resources()
	cmd := exec.Command(
		"qsub",
		"-N", jobName,
		"-o", ctx.job.Stdout,
		"-j", "oe",
		"-l", fmt.Sprintf("select=1:ncpus=%d:mem=%dgb", resources.CPUs, resources.Memory),
		"-l", fmt.Sprintf("walltime=%02d:00:00", resources.Time),
		"--",
		"/bin/bash",
		ctx.script,
	)
	ctx.job.BatchCommand = strings.Join(cmd.Args, " ")
	cmd.Dir = ctx.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("unable to start job: %v: %v: %v", ctx.job.UUID, err, string(out))
	}
	ctx.job.ID = strings.TrimSuffix(string(out), "\n")
	r.jobIDs[ctx.job.UUID] = ctx.job.ID
	return nil
}

func (r *PBSRunner) Completed(j *job) (bool, error) {
	// jobId := r.jobIDs[j.ID]
	jobId := j.ID
	q, err := qstat(jobId)
	if err != nil {
		return false, err
	}
	return q.State == "F", nil
}

func (r *PBSRunner) CompletedSuccessfully(j *job) (bool, error) {
	if j.ID == "" {
		return false, fmt.Errorf("job has no ID")
	}
	q, err := qstat(j.ID)
	if err != nil {
		return false, err
	}
	if q.State == "F" {
		return q.ExitStatus == 0, nil
	} else {
		return false, fmt.Errorf("job has not completed")
	}
}

func (r *PBSRunner) ResourcesUsed(j *job) (resourcesUsed, error) {
	q, err := qstat(j.ID)
	if err != nil {
		return resourcesUsed{}, err
	}
	memUsed, err := convertMemory(q.ResourcesUsed.Mem)
	if err != nil {
		return resourcesUsed{}, err
	}
	wallTimeUsed, err := convertWalltime(q.ResourcesUsed.Walltime)
	if err != nil {
		return resourcesUsed{}, err
	}
	memRequested, err := convertMemory(q.ResourceList.Mem)
	if err != nil {
		return resourcesUsed{}, err
	}
	wallTimeRequested, err := convertWalltime(q.ResourceList.Walltime)
	if err != nil {
		return resourcesUsed{}, err
	}

	return resourcesUsed{
		CPUPercent:      q.ResourcesUsed.Cpupercent,
		MemoryUsed:      memUsed,
		TimeUsed:        wallTimeUsed,
		CPURequested:    q.ResourceList.Ncpus,
		MemoryRequested: memRequested,
		TimeRequested:   wallTimeRequested,
		ExecHost:        q.ExecHost,
		ExitStatus:      q.ExitStatus,
	}, nil
}

func (r *PBSRunner) Kill(j *job) error {
	if j.ID == "" {
		return errors.New("job has no ID")
	}
	cmd := exec.Command("qdel", j.ID)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("unable to kill job %s: %v", j.ID, err)
	}
	return nil
}

type qstatResultList struct {
	Jobs map[string]qstatResult
}

type qstatResult struct {
	Name          string             `json:"Job_Name"`
	State         string             `json:"job_state"`
	ExitStatus    int                `json:"Exit_status"`
	ExecHost      string             `json:"exec_host"`
	OutputPath    string             `json:"Output_Path"`
	ErrorPath     string             `json:"Error_Path"`
	Ctime         string             `json:"ctime"`
	Stime         string             `json:"stime"`
	Mtime         string             `json:"mtime"`
	Qtime         string             `json:"qtime"`
	Etime         string             `json:"etime"`
	ResourcesUsed qstatResourcesUsed `json:"resources_used"`
	ResourceList  qstatResourceList  `json:"Resource_List"`
}

type qstatResourcesUsed struct {
	Cpupercent int    `json:"cpupercent"`
	Cput       string `json:"cput"`
	Mem        string `json:"mem"`
	Ncpus      int    `json:"ncpus"`
	Vmem       string `json:"vmem"`
	Walltime   string `json:"walltime"`
}

type qstatResourceList struct {
	Mem      string `json:"mem"`
	Ncpus    int    `json:"ncpus"`
	Select   string `json:"select"`
	Walltime string `json:"walltime"`
}

func qstat(jobId string) (qstatResult, error) {
	if jobId == "" {
		return qstatResult{}, fmt.Errorf("job has no id")
	}
	cmd := exec.Command("qstat", "-xf", "-F", "json", jobId)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// This is being returned frequently
		time.Sleep(10 * time.Second)
		cmd := exec.Command("qstat", "-xf", "-F", "json", jobId)
		out, err = cmd.CombinedOutput()
		if err != nil {
			time.Sleep(1 * time.Minute)
			cmd := exec.Command("qstat", "-xf", "-F", "json", jobId)
			out, err = cmd.CombinedOutput()
			if err != nil {
				// OK, now something appears to be wrong...
				return qstatResult{}, fmt.Errorf("failed determine job state: failed to run qstat: %v: %s", err, string(out))
			}
		}
	}
	var q qstatResultList
	err = json.Unmarshal(out, &q)
	if err != nil {
		return qstatResult{}, fmt.Errorf("failed to determine job state: failed to unmarshal JSON: %v", err)
	}
	i, ok := q.Jobs[jobId]
	if !ok {
		return qstatResult{}, fmt.Errorf("failed to determine job state: qstat did not contain job id %s", jobId)
	}
	return i, nil
}

func convertMemory(s string) (int, error) {
	for _, suffix := range []string{"gb", "mb", "kb", "b"} {
		if strings.HasSuffix(s, suffix) {
			x, err := strconv.Atoi(strings.TrimSuffix(s, suffix))
			if err != nil {
				return 0, fmt.Errorf("unable to convert memory used: %s: %v", s, err)
			}
			switch suffix {
			case "gb":
				return x, nil
			case "mb":
				return x / 1024, nil
			case "kb":
				return x / 1024 / 1024, nil
			case "b":
				return x / 1024 / 1024 / 1024, nil
			}
		}
	}
	return 0, fmt.Errorf("unexpected suffix: %s", s)
}

func convertWalltime(s string) (int, error) {
	bits := strings.Split(s, ":")
	if len(bits) != 3 {
		return 0, fmt.Errorf("time string has unexpected format: %s", s)
	}
	hours, err := strconv.Atoi(bits[0])
	if err != nil {
		return 0, fmt.Errorf("unable to convert hours to int: %v", err)
	}
	minutes, err := strconv.Atoi(bits[1])
	if err != nil {
		return 0, fmt.Errorf("unable to convert minutes to int: %v", err)
	}
	seconds, err := strconv.Atoi(bits[2])
	if err != nil {
		return 0, fmt.Errorf("unable to convert seconds to int: %v", err)
	}
	return (hours * 60 * 60) + (minutes * 60) + seconds, nil
}
