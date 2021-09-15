package flow

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

var v *viper.Viper

type Commander interface {
	AnalysisName() string
	Command() string
	Resources() (Resources, error)
}

type Resources struct {
	CPUs                 int
	Memory               int
	Time                 int
	Container            string
	SingularityExtraArgs string
}

type Queue struct {
	tasks []Commander
}

func (q *Queue) Add(task Commander) {
	q.tasks = append(q.tasks, task)
}

func (q *Queue) Run() error {
	if !v.IsSet("flowdir") {
		InitConfig("", map[string]interface{}{})
	}
	if len(q.tasks) > 0 {
		log.Printf("Starting workflow with %d jobs", len(q.tasks))
	} else {
		log.Printf("No jobs where added to the queue, nothing to do!")
	}
	for _, task := range q.tasks {
		freezeTask(task)
		r, err := task.Resources()
		if err != nil {
			return fmt.Errorf("failed to get resources for job: %s", task.AnalysisName())
		}
		if r.Container == "" {
			return fmt.Errorf("no container specified for task: %v", task.AnalysisName())
		}
	}
	g, err := newGraph(q.tasks)
	if err != nil {
		return fmt.Errorf("unable to create graph: %v", err)
	}
	err = g.Process()
	if err != nil {
		log.Fatalf("Failed to run workflow: %v", err)
	}
	return nil
}

func freezeTask(c Commander) {
	v := reflect.ValueOf(c).Elem()
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		ft := t.Field(i)
		tag := ft.Tag.Get("type")
		if tag == "input" || tag == "output" {
			val := v.Field(i)
			if val.CanSet() {
				switch kind := val.Kind(); kind {
				case reflect.String:
					p, _ := filepath.Abs(val.String())
					val.SetString(p)
				case reflect.Slice:
					if val.Type().Elem().Name() != "string" {
						panic("tag type:input or type:output on something that is not []string")
					}
					for j := 0; j < val.Len(); j++ {
						sliceValue := val.Index(j)
						p, _ := filepath.Abs(sliceValue.String())
						sliceValue.SetString(p)
					}
				default:
					panic("tag type:input or tag:output on something that is not a string or slice")
				}
			}
		}
	}
}

func ResourcesFor(analysisName string) (Resources, error) {
	// Should we provide default resource allocations or just fail?
	// cpus=1;mem=1;time=1 is rarely going to be useful.
	cpus := v.GetInt(fmt.Sprintf("resources.%s.cpus", analysisName))
	if cpus == 0 {
		return Resources{}, fmt.Errorf("no cpus resource for %s", analysisName)
	}
	memory := v.GetInt(fmt.Sprintf("resources.%s.memory", analysisName))
	if memory == 0 {
		return Resources{}, fmt.Errorf("no memory resource for %s", analysisName)
	}
	time := v.GetInt(fmt.Sprintf("resources.%s.time", analysisName))
	if time == 0 {
		return Resources{}, fmt.Errorf("no time resource for %s", analysisName)
	}
	container := v.GetString(fmt.Sprintf("resources.%s.container", analysisName))
	if container == "" {
		return Resources{}, fmt.Errorf("no container resource for %s", analysisName)
	}
	return Resources{
		CPUs:      cpus,
		Memory:    memory,
		Time:      time,
		Container: container,
	}, nil
}

func InitConfig(fn string, overrides map[string]interface{}) error {
	defaults := map[string]interface{}{
		"flowdir":            ".flow",
		"start_from_scratch": false,
		"job_runner":         "local",
		"singularity_bin":    "singularity",
	}
	v = viper.New()
	for key, value := range defaults {
		v.SetDefault(key, value)
	}
	v.SetConfigName("flow")
	v.SetConfigType("yaml")
	v.AddConfigPath("$HOME/.config/flow")
	v.SetEnvPrefix("flow")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Config found but another error was produced
			return fmt.Errorf("failed to read config file: %v", err)
		}
	}
	if fn != "" {
		localconfig := viper.New()
		localconfig.SetConfigFile(fn)
		localconfig.SetEnvPrefix("flow")
		localconfig.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		localconfig.AutomaticEnv()
		if err := localconfig.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return fmt.Errorf("failed to read local config file: %v", err)
			}
		}
		for _, key := range localconfig.AllKeys() {
			v.Set(key, localconfig.Get(key))
		}
	}
	for key, value := range overrides {
		v.Set(key, value)
	}
	err := os.MkdirAll(v.GetString("flowdir"), 0755)
	if err != nil {
		return fmt.Errorf("failed to create flowdir: %s: %v", v.GetString("flowdir"), err)
	}
	return nil
}

// should this be in the flow package to make in easier for users to run workflows?
func RunWorkflow(fn string) error {
	if !v.IsSet("flowdir") {
		// If flowdir is not set, config has not been initialised. Should we
		// return an error and force the user to init the config?
		InitConfig("", map[string]interface{}{})
	}
	workflowFunc, err := loadPlugin(fn)
	if err != nil {
		return fmt.Errorf("failed to load workflow: %v", err)
	}
	queue := &Queue{}
	workflowFunc(queue)
	if err := queue.Run(); err != nil {
		return err
	}
	return nil
}

func nilWorkflowFunc(q *Queue) {}

func loadPlugin(fn string) (func(*Queue), error) {
	log.Printf("Compiling workflow\n")
	pluginFile, err := compileWorkflow(fn)
	if err != nil {
		return nilWorkflowFunc, fmt.Errorf("failed to compile workflow: %v", err)
	}
	p, err := plugin.Open(pluginFile)
	if err != nil {
		return nilWorkflowFunc, fmt.Errorf("failed to open plugin: %v", err)
	}
	pWorkflow, err := p.Lookup("Workflow")
	if err != nil {
		return nilWorkflowFunc, fmt.Errorf("failed to find Workflow function in plugin: %v", err)
	}
	workflowFunc, ok := pWorkflow.(func(*Queue))
	if !ok {
		return nilWorkflowFunc, fmt.Errorf("workflow func found, but it's type is %T", pWorkflow)
	}
	return workflowFunc, nil
}

func compileWorkflow(fn string) (string, error) {
	dir, err := ioutil.TempDir(v.GetString("flowdir"), "workflow")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %v", err)
	}
	if err := copyFile(fn, fmt.Sprintf("%s/workflow.go", dir)); err != nil {
		return "", fmt.Errorf("failed to copy workflow to temp directory: %v", err)
	}
	// c := exec.Command("go", "mod", "init", "github.com/jje42/workflow")
	// c.Dir = dir
	// if err := c.Run(); err != nil {
	// 	return "", fmt.Errorf("failed to create go.mod: %v", err)
	// }
	// c = exec.Command("go", "mod", "tidy")
	// c.Dir = dir
	// if err := c.Run(); err != nil {
	// 	return "", fmt.Errorf("failed to run go mod tidy: %v", err)
	// }

	cmdl := exec.Command("go", "build", "-buildmode=plugin", "workflow.go")
	cmdl.Dir = dir
	out, err := cmdl.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to compile workflow: %v\n%v", err, string(out))
	}
	return filepath.Join(dir, "workflow.so"), nil
}

func copyFile(src, dst string) error {
	r, err := os.Open(src)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	return err
}

func SafeWriteConfigAs(fn string) error {
	return v.SafeWriteConfigAs(fn)
}
