package compact_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/printlog/compact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

func makeSpawnExec(inputs, outputs []string) *spb.SpawnExec {
	actualOutputs := make([]*spb.File, 0, len(outputs))
	listedOutputs := make([]string, 0, len(outputs))
	for _, o := range outputs {
		actualOutputs = append(actualOutputs, &spb.File{Path: o})
		listedOutputs = append(listedOutputs, o)
	}

	inputFiles := make([]*spb.File, 0, len(inputs))
	for _, i := range inputs {
		inputFiles = append(inputFiles, &spb.File{Path: i})
	}

	return &spb.SpawnExec{
		ActualOutputs: actualOutputs,
		ListedOutputs: listedOutputs,
		Inputs:        inputFiles,
	}
}

func validateStableSort(t *testing.T, preSorts []*spb.SpawnExec, expected []*spb.SpawnExec) {
	sortedSpawns := make([]*spb.SpawnExec, 0, len(preSorts))
	collectSpawns := func(s *spb.SpawnExec) error {
		sortedSpawns = append(sortedSpawns, s)
		return nil
	}

	err := compact.StableSortExec(preSorts, collectSpawns)
	require.NoError(t, err)
	require.Equal(t, len(expected), len(sortedSpawns), "expected and actual have different lengths, preSorts lenth is %d", len(preSorts))
	for i := range sortedSpawns {
		assert.Same(t, expected[i], sortedSpawns[i], fmt.Sprintf("two spawns with index %d are not the same:\nEXPECTED: %v\n---\nACTUAL: %v", i, expected[i], sortedSpawns[i]))
	}
}

func TestStableSortEmpty(t *testing.T) {
	preSorts := []*spb.SpawnExec{}
	expected := []*spb.SpawnExec{}
	validateStableSort(t, preSorts, expected)
}

func TestStableSortOne(t *testing.T) {
	a := makeSpawnExec([]string{}, []string{"a"})

	preSorts := []*spb.SpawnExec{a}
	expected := []*spb.SpawnExec{a}
	validateStableSort(t, preSorts, expected)
}

func TestStableSortUnlinkedLexicographic(t *testing.T) {
	a := makeSpawnExec([]string{"leaf1"}, []string{"a"})
	b := makeSpawnExec([]string{"leaf2"}, []string{"b"})

	t.Run("default", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{a, b}
		expected := []*spb.SpawnExec{a, b}
		validateStableSort(t, preSorts, expected)
	})
	t.Run("reverse", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{b, a}
		expected := []*spb.SpawnExec{a, b}
		validateStableSort(t, preSorts, expected)
	})
}

func TestStableSortLinked(t *testing.T) {
	e1 := makeSpawnExec(
		[]string{"leaf1"},
		[]string{"b"},
	)
	e2 := makeSpawnExec(
		[]string{"b"},
		[]string{"a"},
	)

	t.Run("default", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{e1, e2}
		expected := []*spb.SpawnExec{e1, e2}
		validateStableSort(t, preSorts, expected)
	})
	t.Run("reverse", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{e2, e1}
		expected := []*spb.SpawnExec{e1, e2}
		validateStableSort(t, preSorts, expected)
	})
}

func TestStableSortMultipleOutputs(t *testing.T) {
	m1 := makeSpawnExec([]string{"leaf"}, []string{"b1", "b2", "b3"})
	m2 := makeSpawnExec([]string{"b2"}, []string{"a"})
	m3 := makeSpawnExec([]string{"b2", "b3"}, []string{"a"})
	m4 := makeSpawnExec([]string{"z", "b2", "1"}, []string{"a"})

	t.Run("one_of", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{m2, m1}
		expected := []*spb.SpawnExec{m1, m2}
		validateStableSort(t, preSorts, expected)
	})
	t.Run("many_of", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{m3, m1}
		expected := []*spb.SpawnExec{m1, m3}
		validateStableSort(t, preSorts, expected)
	})
	t.Run("irrelevant_inputs", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{m4, m1}
		expected := []*spb.SpawnExec{m1, m4}
		validateStableSort(t, preSorts, expected)
	})
}

func TestStableSortThree(t *testing.T) {
	for _, tc := range []struct {
		name     string
		a, b, c  *spb.SpawnExec
		expected []string
	}{
		{
			name:     "ABC",
			a:        makeSpawnExec([]string{}, []string{"a"}),
			b:        makeSpawnExec([]string{}, []string{"b"}),
			c:        makeSpawnExec([]string{}, []string{"c"}),
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "CBA",
			a:        makeSpawnExec([]string{"b"}, []string{"a"}),
			b:        makeSpawnExec([]string{"c"}, []string{"b"}),
			c:        makeSpawnExec([]string{}, []string{"c"}),
			expected: []string{"c", "b", "a"},
		},
		{
			name:     "ACB",
			a:        makeSpawnExec([]string{}, []string{"a"}),
			b:        makeSpawnExec([]string{"a", "c"}, []string{"b"}),
			c:        makeSpawnExec([]string{}, []string{"c"}),
			expected: []string{"a", "c", "b"},
		},
		{
			name:     "CAB",
			a:        makeSpawnExec([]string{"c"}, []string{"a"}),
			b:        makeSpawnExec([]string{"c"}, []string{"b"}),
			c:        makeSpawnExec([]string{}, []string{"c"}),
			expected: []string{"c", "a", "b"},
		},
		{
			name:     "CAB2",
			a:        makeSpawnExec([]string{"c1"}, []string{"a"}),
			b:        makeSpawnExec([]string{"c2"}, []string{"b"}),
			c:        makeSpawnExec([]string{}, []string{"c1", "c2"}),
			expected: []string{"c", "a", "b"},
		},
		{
			name:     "duplicate_outputs",
			a:        makeSpawnExec([]string{"a"}, []string{"c"}),
			b:        makeSpawnExec([]string{"b"}, []string{"c"}),
			c:        makeSpawnExec([]string{"c"}, []string{"d"}),
			expected: []string{"a", "b", "c"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			preSorts := []*spb.SpawnExec{tc.a, tc.b, tc.c}
			expected := []*spb.SpawnExec{}
			for _, name := range tc.expected {
				switch name {
				case "a":
					expected = append(expected, tc.a)
				case "b":
					expected = append(expected, tc.b)
				case "c":
					expected = append(expected, tc.c)
				default:
					t.Fail()
				}
			}
			validateStableSort(t, preSorts, expected)
		})
	}
}

func TestStableSortSix(t *testing.T) {
	for _, tc := range []struct {
		name             string
		a, b, c, d, e, f *spb.SpawnExec
		expected         []string
	}{
		{
			name:     "CBAFED",
			a:        makeSpawnExec([]string{"b"}, []string{"a"}),
			b:        makeSpawnExec([]string{"c"}, []string{"b"}),
			c:        makeSpawnExec([]string{}, []string{"c"}),
			d:        makeSpawnExec([]string{"e"}, []string{"d"}),
			e:        makeSpawnExec([]string{"f"}, []string{"e"}),
			f:        makeSpawnExec([]string{}, []string{"f"}),
			expected: []string{"c", "b", "a", "f", "e", "d"},
		},
		{
			name:     "interleaved_paths",
			a:        makeSpawnExec([]string{"c"}, []string{"a"}),
			b:        makeSpawnExec([]string{}, []string{"b"}),
			c:        makeSpawnExec([]string{}, []string{"c"}),
			d:        makeSpawnExec([]string{"a"}, []string{"d"}),
			e:        makeSpawnExec([]string{"f"}, []string{"e"}),
			f:        makeSpawnExec([]string{"b"}, []string{"f"}),
			expected: []string{"b", "c", "a", "d", "f", "e"},
		},
		{
			name:     "many_deps",
			a:        makeSpawnExec([]string{"b", "c", "f"}, []string{"a"}),
			b:        makeSpawnExec([]string{"d", "e"}, []string{"b"}),
			c:        makeSpawnExec([]string{"e", "d", "f"}, []string{"c"}),
			d:        makeSpawnExec([]string{}, []string{"d"}),
			e:        makeSpawnExec([]string{"f"}, []string{"e"}),
			f:        makeSpawnExec([]string{}, []string{"f"}),
			expected: []string{"d", "f", "e", "b", "c", "a"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			preSorts := []*spb.SpawnExec{tc.a, tc.b, tc.c, tc.d, tc.e, tc.f}
			expected := []*spb.SpawnExec{}
			for _, name := range tc.expected {
				switch name {
				case "a":
					expected = append(expected, tc.a)
				case "b":
					expected = append(expected, tc.b)
				case "c":
					expected = append(expected, tc.c)
				case "d":
					expected = append(expected, tc.d)
				case "e":
					expected = append(expected, tc.e)
				case "f":
					expected = append(expected, tc.f)
				default:
					t.Fail()
				}
			}
			validateStableSort(t, preSorts, expected)
		})
	}
}

func TestStableSortNoOutputs(t *testing.T) {
	a := makeSpawnExec([]string{"a"}, []string{})
	b := makeSpawnExec([]string{"b"}, []string{})

	t.Run("default", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{a, b}
		expected := []*spb.SpawnExec{a, b}
		validateStableSort(t, preSorts, expected)
	})
	t.Run("reverse", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{b, a}
		expected := []*spb.SpawnExec{a, b}
		validateStableSort(t, preSorts, expected)
	})
}

func TestStableSortListedOutputs(t *testing.T) {
	a := makeSpawnExec([]string{}, []string{"a"})
	a.CommandArgs = []string{"a"}
	b := makeSpawnExec([]string{}, []string{})
	b.CommandArgs = []string{"b"}
	c := makeSpawnExec([]string{}, []string{"c"})
	c.CommandArgs = []string{"c"}
	d := makeSpawnExec([]string{}, []string{"d"})
	d.CommandArgs = []string{"d"}
	e := makeSpawnExec([]string{}, []string{})
	e.CommandArgs = []string{"e"}
	f := makeSpawnExec([]string{}, []string{})
	f.CommandArgs = []string{"f"}

	t.Run("default", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{a, b, c, d, e, f}
		expected := []*spb.SpawnExec{
			// sorted spawns with actual outputs
			a,
			c,
			d,
			// sorted spawns with listed outputs
			b,
			e,
			f,
		}
		validateStableSort(t, preSorts, expected)
	})
	t.Run("reverse", func(t *testing.T) {
		preSorts := []*spb.SpawnExec{f, e, d, c, b, a}
		expected := []*spb.SpawnExec{
			// sorted spawns with actual outputs
			a,
			c,
			d,
			// sorted spawns with listed outputs
			b,
			e,
			f,
		}
		validateStableSort(t, preSorts, expected)
	})
	t.Run("dependencies", func(t *testing.T) {
		a.Inputs = []*spb.File{{Path: "d"}}
		c.Inputs = []*spb.File{{Path: "d"}}
		preSorts := []*spb.SpawnExec{f, e, d, c, b, a}
		expected := []*spb.SpawnExec{d, a, c, b, e, f}
		validateStableSort(t, preSorts, expected)
	})
}
