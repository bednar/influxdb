package exec

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/generate"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/profile"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/pkg/data/gen"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "store query".
type Command struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	deps   Dependencies
	server server.Interface
	filter SeriesGeneratorFilter

	configPath  string
	printOnly   bool
	noTSI       bool
	concurrency int
	schemaPath  string
	storageSpec generate.StorageSpec
	schemaSpec  generate.SchemaSpec

	profile profile.Config
}

type SeriesGeneratorFilter func(sgi meta.ShardGroupInfo, g gen.SeriesGenerator) gen.SeriesGenerator

type Dependencies struct {
	Server server.Interface

	// SeriesGeneratorFilter wraps g with a SeriesGenerator that
	// returns a subset of keys from g
	SeriesGeneratorFilter SeriesGeneratorFilter
}

// NewCommand returns a new instance of Command.
func NewCommand(deps Dependencies) *Command {
	return &Command{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		server: deps.Server,
		filter: deps.SeriesGeneratorFilter,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}

	storagePlan, err := cmd.storageSpec.Plan(cmd.server)
	if err != nil {
		return err
	}

	storagePlan.PrintPlan(cmd.Stdout)

	var spec *gen.Spec
	if cmd.schemaPath != "" {
		var err error
		spec, err = gen.NewSpecFromPath(cmd.schemaPath)
		if err != nil {
			return err
		}
	} else {
		schemaPlan, err := cmd.schemaSpec.Plan(storagePlan)
		if err != nil {
			return err
		}

		schemaPlan.PrintPlan(cmd.Stdout)
		spec = cmd.planToSpec(schemaPlan)
	}

	if cmd.printOnly {
		return nil
	}

	if err = storagePlan.InitFileSystem(cmd.server.MetaClient()); err != nil {
		return err
	}

	return cmd.exec(storagePlan, spec)
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("gen-init", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.schemaPath, "schema", "", "Schema TOML file")
	fs.BoolVar(&cmd.printOnly, "print", false, "Print data spec only")
	fs.BoolVar(&cmd.noTSI, "no-tsi", false, "Skip building TSI index")
	fs.IntVar(&cmd.concurrency, "c", 1, "Number of shards to generate concurrently")
	fs.StringVar(&cmd.profile.CPU, "cpuprofile", "", "Collect a CPU profile")
	fs.StringVar(&cmd.profile.Memory, "memprofile", "", "Collect a memory profile")
	cmd.storageSpec.AddFlags(fs)
	cmd.schemaSpec.AddFlags(fs)

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.storageSpec.Database == "" {
		return errors.New("database is required")
	}

	if cmd.storageSpec.Retention == "" {
		return errors.New("retention policy is required")
	}

	return nil
}

var (
	tomlSchema = template.Must(template.New("schema").Parse(`
title = "CLI schema"

[[measurements]]
name = "m0"
tags = [
{{- range $i, $e := .Tags }}
	{ name = "tag{{$i}}", source = { type = "sequence", format = "value%s", start = 0, count = {{$e}} } },{{ end }}
]
fields = [
	{ name = "v0", count = {{ .PointsPerSeriesPerShard }}, source = 1.0 },
]`))
)

func (cmd *Command) planToSpec(p *generate.SchemaPlan) *gen.Spec {
	var sb strings.Builder
	if err := tomlSchema.Execute(&sb, p); err != nil {
		panic(err)
	}

	spec, err := gen.NewSpecFromToml(sb.String())
	if err != nil {
		panic(err)
	}
	return spec
}

func (cmd *Command) exec(storagePlan *generate.StoragePlan, spec *gen.Spec) error {
	groups := storagePlan.ShardGroups()
	gens := make([]gen.SeriesGenerator, len(groups))
	for i := range gens {
		sgi := groups[i]
		tr := gen.TimeRange{
			Start: sgi.StartTime,
			End:   sgi.EndTime,
		}
		gens[i] = gen.NewSeriesGeneratorFromSpec(spec, tr)
		if cmd.filter != nil {
			gens[i] = cmd.filter(sgi, gens[i])
		}
	}

	stop := cmd.profile.Start()
	defer stop()

	start := time.Now().UTC()
	defer func() {
		elapsed := time.Since(start)
		fmt.Println()
		fmt.Printf("Total time: %0.1f seconds\n", elapsed.Seconds())
	}()

	g := Generator{Concurrency: cmd.concurrency, BuildTSI: !cmd.noTSI}
	return g.Run(context.Background(), storagePlan.Database, storagePlan.ShardPath(), storagePlan.NodeShardGroups(), gens)
}
