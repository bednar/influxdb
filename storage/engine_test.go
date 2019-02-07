package storage_test

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/tsdb"
)

func TestEngine_WriteAndIndex(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()

	// Calling WritePoints when the engine is not open will return
	// ErrEngineClosed.
	if got, exp := engine.Engine.WritePoints(nil), storage.ErrEngineClosed; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.Tags{{Key: []byte("host"), Value: []byte("server")}},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints([]models.Point{pt}); err != nil {
		t.Fatal(err)
	}

	pt.SetTime(time.Unix(2, 3))
	if err := engine.Engine.WritePoints([]models.Point{pt}); err != nil {
		t.Fatal(err)
	}

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %v series, exp %v series in index", got, exp)
	}

	// ensure the index gets loaded after closing and opening the shard
	engine.Engine.Close() // Don't remove the data
	engine.MustOpen()

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %v series, exp %v series in index", got, exp)
	}

	// and ensure that we can still write data
	pt.SetTime(time.Unix(2, 6))
	if err := engine.Engine.WritePoints([]models.Point{pt}); err != nil {
		t.Fatal(err)
	}
}

func TestEngine_TimeTag(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"time": "value"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints([]models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

	pt = models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"foo": "bar", "time": "value"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints([]models.Point{pt}); err == nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWrite_TimeField(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	name := tsdb.EncodeNameString(engine.org, engine.bucket)

	pt := models.MustNewPoint(
		name,
		models.NewTags(map[string]string{"_f": "time", "_m": "cpu"}),
		map[string]interface{}{"time": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints([]models.Point{pt}); err == nil {
		t.Fatal("expected error: got nil")
	}

	var points []models.Point
	points = append(points, models.MustNewPoint(
		name,
		models.NewTags(map[string]string{"_f": "time", "_m": "cpu"}),
		map[string]interface{}{"time": 1.0},
		time.Unix(1, 2),
	))
	points = append(points, models.MustNewPoint(
		name,
		models.NewTags(map[string]string{"_f": "value", "_m": "cpu"}),
		map[string]interface{}{"value": 1.1},
		time.Unix(1, 2),
	))

	if err := engine.Engine.WritePoints(points); err == nil {
		t.Fatal("expected error: got nil")
	}
}

func TestEngine_WriteAddNewField(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	name := tsdb.EncodeNameString(engine.org, engine.bucket)

	if err := engine.Engine.WritePoints([]models.Point{models.MustNewPoint(
		name,
		models.NewTags(map[string]string{"_f": "value", "_m": "cpu", "host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)}); err != nil {
		t.Fatalf(err.Error())
	}

	if err := engine.Engine.WritePoints([]models.Point{
		models.MustNewPoint(
			name,
			models.NewTags(map[string]string{"_f": "value", "_m": "cpu", "host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 2),
		),
		models.MustNewPoint(
			name,
			models.NewTags(map[string]string{"_f": "value2", "_m": "cpu", "host": "server"}),
			map[string]interface{}{"value2": 2.0},
			time.Unix(1, 2),
		),
	}); err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := engine.SeriesCardinality(), int64(2); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

func TestEngine_DeleteBucket(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	orgID, _ := influxdb.IDFromString("3131313131313131")
	bucketID, _ := influxdb.IDFromString("8888888888888888")

	err := engine.Engine.WritePoints([]models.Point{models.MustNewPoint(
		tsdb.EncodeNameString(engine.org, engine.bucket),
		models.NewTags(map[string]string{"_f": "value", "_m": "cpu", "host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)})
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Same org, different bucket.
	err = engine.Engine.WritePoints([]models.Point{
		models.MustNewPoint(
			tsdb.EncodeNameString(*orgID, *bucketID),
			models.NewTags(map[string]string{"_f": "value", "_m": "cpu", "host": "server"}),
			map[string]interface{}{"value": 1.0},
			time.Unix(1, 3),
		),
		models.MustNewPoint(
			tsdb.EncodeNameString(*orgID, *bucketID),
			models.NewTags(map[string]string{"_f": "value2", "_m": "cpu", "host": "server"}),
			map[string]interface{}{"value2": 2.0},
			time.Unix(1, 3),
		),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := engine.SeriesCardinality(), int64(3); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// Remove the original bucket.
	if err := engine.DeleteBucket(engine.org, engine.bucket); err != nil {
		t.Fatal(err)
	}

	// Check only one bucket was removed.
	if got, exp := engine.SeriesCardinality(), int64(2); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

func TestEngine_OpenClose(t *testing.T) {
	engine := NewDefaultEngine()
	engine.MustOpen()

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	if err := engine.Open(); err != nil {
		t.Fatal(err)
	}

	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensures that when a shard is closed, it removes any series meta-data
// from the index.
func TestEngineClose_RemoveIndex(t *testing.T) {
	engine := NewDefaultEngine()
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	err := engine.Engine.WritePoints([]models.Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}

	// ensure the index gets loaded after closing and opening the shard
	engine.Engine.Close() // Don't destroy temporary data.
	engine.Open()

	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %d series, exp %d series in index", got, exp)
	}
}

func TestEngine_WALDisabled(t *testing.T) {
	config := storage.NewConfig()
	config.WAL.Enabled = false

	engine := NewEngine(config)
	defer engine.Close()
	engine.MustOpen()

	pt := models.MustNewPoint(
		"cpu",
		models.NewTags(map[string]string{"host": "server"}),
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)

	if err := engine.Engine.WritePoints([]models.Point{pt}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDeleteBucket(b *testing.B) {
	var engine *Engine
	setup := func(card int) {
		engine = NewDefaultEngine()
		engine.MustOpen()

		points := make([]models.Point, card)
		for i := 0; i < card; i++ {
			points[i] = models.MustNewPoint(
				"cpu",
				models.NewTags(map[string]string{"host": "server"}),
				map[string]interface{}{"value": i},
				time.Unix(1, 2),
			)
		}

		if err := engine.Engine.WritePoints(points); err != nil {
			panic(err)
		}
	}

	for i := 1; i <= 5; i++ {
		card := int(math.Pow10(i))

		b.Run(fmt.Sprintf("cardinality_%d", card), func(b *testing.B) {
			setup(card)
			for i := 0; i < b.N; i++ {
				if err := engine.DeleteBucket(engine.org, engine.bucket); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				if err := engine.Close(); err != nil {
					panic(err)
				}
				setup(card)
				b.StartTimer()
			}
		})

	}
}

type Engine struct {
	path        string
	org, bucket influxdb.ID

	*storage.Engine
}

// NewEngine create a new wrapper around a storage engine.
func NewEngine(c storage.Config) *Engine {
	path, _ := ioutil.TempDir("", "storage_engine_test")

	engine := storage.NewEngine(path, c)

	org, err := influxdb.IDFromString("3131313131313131")
	if err != nil {
		panic(err)
	}

	bucket, err := influxdb.IDFromString("3232323232323232")
	if err != nil {
		panic(err)
	}

	return &Engine{
		path:   path,
		org:    *org,
		bucket: *bucket,
		Engine: engine,
	}
}

// NewDefaultEngine returns a new Engine with a default configuration.
func NewDefaultEngine() *Engine {
	return NewEngine(storage.NewConfig())
}

// MustOpen opens the engine or panicks.
func (e *Engine) MustOpen() {
	if err := e.Engine.Open(); err != nil {
		panic(err)
	}
}

// Close closes the engine and removes all temporary data.
func (e *Engine) Close() error {
	defer os.RemoveAll(e.path)
	return e.Engine.Close()
}
