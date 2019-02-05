package gen

import (
	"math/rand"
)

// produces floats using the linear equation ax + b, where x is a float value
// from [0, 1)
type floatRandomValuesSequence struct {
	r *rand.Rand
	a float64
	b float64
}

func NewFloatRandomValuesSequence(min, max float64, r *rand.Rand) FloatValuesSequence {
	return &floatRandomValuesSequence{r: r, a: max - min, b: min}
}

func (g *floatRandomValuesSequence) Reset() {}

func (g *floatRandomValuesSequence) Write(vs []float64) {
	var (
		a = g.a
		b = g.b
	)
	for i := 0; i < len(vs); i++ {
		vs[i] = a*g.r.Float64() + b // ax + b
	}
}
