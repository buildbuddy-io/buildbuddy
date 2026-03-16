package typescript

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticImportPattern(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "default import",
			input:    `import React from 'react'`,
			expected: []string{"react"},
		},
		{
			name:     "named imports",
			input:    `import { useState, useEffect } from 'react'`,
			expected: []string{"react"},
		},
		{
			name:     "namespace import",
			input:    `import * as React from 'react'`,
			expected: []string{"react"},
		},
		{
			name:     "side-effect import",
			input:    `import './styles.css'`,
			expected: []string{"./styles.css"},
		},
		{
			name:     "type import",
			input:    `import type { Foo } from './types'`,
			expected: []string{"./types"},
		},
		{
			name:     "default and named imports",
			input:    `import React, { useState } from 'react'`,
			expected: []string{"react"},
		},
		{
			name:     "double quoted import",
			input:    `import { foo } from "bar"`,
			expected: []string{"bar"},
		},
		{
			name:     "scoped npm import",
			input:    `import { something } from '@scope/package'`,
			expected: []string{"@scope/package"},
		},
		{
			name:     "relative import",
			input:    `import { Component } from './component'`,
			expected: []string{"./component"},
		},
		{
			name: "multiple imports",
			input: `import React from 'react'
import { render } from 'react-dom'
import './styles.css'`,
			expected: []string{"react", "react-dom", "./styles.css"},
		},
		{
			name:     "no imports",
			input:    `const x = 42;`,
			expected: nil,
		},
		{
			name:     "default and namespace import",
			input:    `import def, * as ns from 'module'`,
			expected: []string{"module"},
		},
		{
			name:     "multiline named imports",
			input:    "import {\n  foo,\n  bar,\n  baz\n} from 'module'",
			expected: []string{"module"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := staticImportPattern.FindAllSubmatch([]byte(tt.input), -1)
			var results []string
			for _, match := range matches {
				results = append(results, string(match[1]))
			}
			assert.Equal(t, tt.expected, results)
		})
	}
}

func TestDynamicImportPattern(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "dynamic import single quotes",
			input:    `const mod = import('./module')`,
			expected: []string{"./module"},
		},
		{
			name:     "dynamic import double quotes",
			input:    `const mod = import("./module")`,
			expected: []string{"./module"},
		},
		{
			name:     "no dynamic imports",
			input:    `import { foo } from 'bar'`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := dynamicImportPattern.FindAllSubmatch([]byte(tt.input), -1)
			var results []string
			for _, match := range matches {
				results = append(results, string(match[2]))
			}
			assert.Equal(t, tt.expected, results)
		})
	}
}
