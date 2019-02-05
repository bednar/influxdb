package gen

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cast"
)

type SeriesLimit int64

func (s *SeriesLimit) UnmarshalTOML(data interface{}) error {
	v, ok := data.(int64)
	if !ok {
		return errors.New("series-limit: invalid value")
	}

	if v < 0 {
		return errors.New("series-limit: must be ≥ 0")
	}

	*s = SeriesLimit(v)
	return nil
}

type precision byte

const (
	precisionMillisecond precision = iota // default
	precisionNanosecond
	precisionMicrosecond
	precisionSecond
	precisionMinute
	precisionHour
)

var precisionToDuration = [...]time.Duration{
	time.Millisecond,
	time.Nanosecond,
	time.Microsecond,
	time.Second,
	time.Minute,
	time.Minute * 60,
	time.Nanosecond,
	time.Nanosecond,
}

func (p *precision) ToDuration() time.Duration {
	return precisionToDuration[*p&0x7]
}

func (p *precision) UnmarshalTOML(data interface{}) error {
	d, ok := data.(string)
	if !ok {
		return fmt.Errorf("invalid precision, expect one of (ns, us, ms, s, m, h): %T", data)
	}

	d = strings.ToLower(d)

	switch d {
	case "ns", "nanosecond":
		*p = precisionNanosecond
	case "us", "microsecond", "µs":
		*p = precisionMicrosecond
	case "ms", "millisecond":
		*p = precisionMillisecond
	case "s", "second":
		*p = precisionSecond
	case "m", "minute":
		*p = precisionMinute
	case "h", "hour":
		*p = precisionHour
	default:
		return fmt.Errorf("invalid precision, expect one of (ns, ms, s, m, h): %s", d)
	}
	return nil
}

func (t *Tag) UnmarshalTOML(data interface{}) error {
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	if n, ok := d["name"].(string); !ok || n == "" {
		return errors.New("tag: missing or invalid value for name")
	} else {
		t.Name = n
	}

	// infer source

	if _, ok := d["source"]; !ok {
		return fmt.Errorf("missing source for tag %q", t.Name)
	}

	switch v := d["source"].(type) {
	case []interface{}:
		if src, err := decodeTagArraySource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	case map[string]interface{}:
		if src, err := decodeTagSource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	default:
		return fmt.Errorf("invalid source for tag %q: %T", t.Name, v)
	}

	return nil
}

func decodeTagArraySource(data []interface{}) (TagSource, error) {
	if len(data) == 0 {
		return nil, errors.New("empty array source")
	}

	if src, err := cast.ToStringSliceE(data); err != nil {
		return nil, err
	} else {
		return &TagArraySource{Values: src}, nil
	}
}

func decodeTagSource(data map[string]interface{}) (TagSource, error) {
	typ, ok := data["type"].(string)
	if !ok {
		return nil, errors.New("missing type field")
	}
	switch typ {
	case "sequence":
		return decodeTagSequenceSource(data)
	case "file":
		return decodeTagFileSource(data)
	default:
		return nil, fmt.Errorf("invalid type field %q", typ)
	}
}

func decodeTagFileSource(data map[string]interface{}) (TagSource, error) {
	var s TagFileSource

	if v, ok := data["path"].(string); ok {
		s.Path = v
	} else {
		return nil, errors.New("file: missing path")
	}

	return &s, nil
}

func decodeTagSequenceSource(data map[string]interface{}) (TagSource, error) {
	var s TagSequenceSource

	if v, ok := data["format"].(string); ok {
		// TODO(sgc): validate format string
		s.Format = v
	} else {
		s.Format = "value%s"
	}

	if v, ok := data["start"]; ok {
		if v, err := cast.ToInt64E(v); err != nil {
			return nil, fmt.Errorf("tag.sequence: invalid start, %v", err)
		} else if v < 0 {
			return nil, fmt.Errorf("tag.sequence: start must be ≥ 0")
		} else {
			s.Start = v
		}
	}

	if v, ok := data["count"]; ok {
		if v, err := cast.ToInt64E(v); err != nil {
			return nil, fmt.Errorf("tag.sequence: invalid count, %v", err)
		} else if v < 0 {
			return nil, fmt.Errorf("tag.sequence: count must be > 0")
		} else {
			s.Count = v
		}
	} else {
		return nil, fmt.Errorf("tag.sequence: missing count")
	}

	return &s, nil
}

func (t *Field) UnmarshalTOML(data interface{}) error {
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	if n, ok := d["name"].(string); !ok || n == "" {
		return errors.New("field: missing or invalid value for name")
	} else {
		t.Name = n
	}

	if n, ok := d["count"]; !ok {
		return errors.New("field: missing value for count")
	} else if count, err := cast.ToInt64E(n); err != nil {
		return fmt.Errorf("field: invalid count, %v", err)
	} else if count <= 0 {
		return errors.New("field: count must be > 0")
	} else {
		t.Count = count
	}

	if n, ok := d["time-precision"]; ok {
		if err := t.TimePrecision.UnmarshalTOML(n); err != nil {
			return err
		}
	}

	// infer source
	if _, ok := d["source"]; !ok {
		return fmt.Errorf("missing source for field %q", t.Name)
	}

	switch v := d["source"].(type) {
	case int64, string, float64, bool:
		t.Source = &FieldConstantValue{v}
	case []interface{}:
		if src, err := decodeFieldArraySource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	case map[string]interface{}:
		if src, err := decodeFieldSource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	default:
		// unknown
		return fmt.Errorf("invalid source for tag %q: %T", t.Name, v)
	}

	return nil
}

func decodeFieldArraySource(data []interface{}) (FieldSource, error) {
	if len(data) == 0 {
		return nil, errors.New("empty array")
	}

	var (
		src interface{}
		err error
	)

	// use first value to determine slice type
	switch data[0].(type) {
	case int64:
		src, err = toInt64SliceE(data)
	case float64:
		src, err = toFloat64SliceE(data)
	case string:
		src, err = cast.ToStringSliceE(data)
	case bool:
		src, err = cast.ToBoolSliceE(data)
	default:
		err = fmt.Errorf("unsupported field source data type: %T", data[0])
	}

	if err != nil {
		return nil, err
	}

	return &FieldArraySource{Value: src}, nil
}

func decodeFieldSource(data map[string]interface{}) (FieldSource, error) {
	typ, ok := data["type"].(string)
	if !ok {
		return nil, errors.New("missing type field")
	}
	switch typ {
	case "rand<float>":
		return decodeFloatRandomSource(data)
	default:
		return nil, fmt.Errorf("invalid type field %q", typ)
	}
}

func decodeFloatRandomSource(data map[string]interface{}) (FieldSource, error) {
	var s FieldFloatRandomSource

	if v, ok := data["seed"]; ok {
		if v, err := cast.ToInt64E(v); err != nil {
			return nil, fmt.Errorf("rand<float>: invalid seed, %v", err)
		} else {
			s.Seed = v
		}
	}

	if v, ok := data["min"]; ok {
		if v, err := cast.ToFloat64E(v); err != nil {
			return nil, fmt.Errorf("rand<float>: invalid min, %v", err)
		} else {
			s.Min = v
		}
	}

	if v, ok := data["max"]; ok {
		if v, err := cast.ToFloat64E(v); err != nil {
			return nil, fmt.Errorf("rand<float>: invalid max, %v", err)
		} else {
			s.Max = v
		}
	} else {
		s.Max = 1.0
	}

	if !(s.Min <= s.Max) {
		return nil, errors.New("rand<float>: min ≤ max")
	}

	return &s, nil
}
