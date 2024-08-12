package execgraph

import (
	"cmp"
	"errors"
	"fmt"
	"golang.org/x/exp/constraints"
	"slices"
)

type Hasher[V any, H constraints.Ordered] func(V) H

type Graph[V, E any, H constraints.Ordered] struct {
	vertices map[H]V
	// Invariant: The inner map is never nil or empty.
	inEdges  map[H]map[H]E
	outEdges map[H]map[H]E

	hasher Hasher[V, H]
}

// NewGraph creates a new empty graph.
// The order of type parameters allows V and H to be inferred.
func NewGraph[E any, V any, H constraints.Ordered](vertexHasher Hasher[V, H]) *Graph[V, E, H] {
	return &Graph[V, E, H]{
		vertices: make(map[H]V),
		inEdges:  make(map[H]map[H]E),
		outEdges: make(map[H]map[H]E),
		hasher:   vertexHasher,
	}
}

func (g *Graph[V, E, H]) AddVertex(vertex V) bool {
	return g.addVertex(g.hasher(vertex), vertex)
}

func (g *Graph[V, E, H]) addVertex(vertexHash H, vertex V) bool {
	if _, ok := g.vertices[vertexHash]; ok {
		return false
	}
	g.vertices[vertexHash] = vertex
	return true
}

var ErrEdgeExists = errors.New("edge already exists")
var ErrEdgeNotFound = errors.New("edge not found")

func (g *Graph[V, E, H]) AddEdge(source, target V, edge E) error {
	sourceHash := g.hasher(source)
	targetHash := g.hasher(target)

	_ = g.addVertex(sourceHash, source)
	_ = g.addVertex(targetHash, target)

	outEdges, ok := g.outEdges[sourceHash]
	if !ok {
		outEdges = make(map[H]E)
		g.outEdges[sourceHash] = outEdges
	}
	inEdges, ok := g.inEdges[targetHash]
	if !ok {
		inEdges = make(map[H]E)
		g.inEdges[targetHash] = inEdges
	}

	if _, ok = outEdges[targetHash]; ok {
		return fmt.Errorf("failed to create edge from %v to %v: %w", sourceHash, targetHash, ErrEdgeExists)
	}

	outEdges[targetHash] = edge
	inEdges[sourceHash] = edge

	return nil
}

func (g *Graph[V, E, H]) Edge(source, target V) (edge E, err error) {
	sourceHash := g.hasher(source)
	targetHash := g.hasher(target)

	outEdges, ok := g.outEdges[sourceHash]
	if !ok {
		err = fmt.Errorf("failed to get edge from %v to %v: %w", sourceHash, targetHash, ErrEdgeNotFound)
		return
	}
	edge, ok = outEdges[targetHash]
	if !ok {
		err = fmt.Errorf("failed to get edge from %v to %v: %w", sourceHash, targetHash, ErrEdgeNotFound)
		return
	}
	return
}

func (g *Graph[V, E, H]) RemoveEdge(source, target V) error {
	sourceHash := g.hasher(source)
	targetHash := g.hasher(target)

	outEdges, ok := g.outEdges[sourceHash]
	if !ok {
		return fmt.Errorf("failed to remove edge from %v to %v: %w", sourceHash, targetHash, ErrEdgeNotFound)
	}
	inEdges, ok := g.inEdges[targetHash]
	if !ok {
		panic(fmt.Sprintf("inEdges[%v] not found but outEdges[%v] found", targetHash, sourceHash))
	}

	delete(outEdges, targetHash)
	if len(outEdges) == 0 {
		delete(g.outEdges, sourceHash)
	}
	delete(inEdges, sourceHash)
	if len(inEdges) == 0 {
		delete(g.inEdges, targetHash)
	}

	return nil
}

func (g *Graph[V, E, H]) Roots() []V {
	var roots []V
	for vertexHash := range g.vertices {
		if _, ok := g.inEdges[vertexHash]; !ok {
			roots = append(roots, g.vertices[vertexHash])
		}
	}
	slices.SortFunc(roots, func(a, b V) int {
		return cmp.Compare(g.hasher(a), g.hasher(b))
	})
	return roots
}
