package main

import (
	"testing"
)

func TestParseLabelConstantWithSingleSpace(t *testing.T) {
	gen := newDocsGenerator("/dev/null")
	gen.state = "LABEL_CONSTANTS"
	if err := gen.processLine(0, `    StatusLabel = "status"`); err != nil {
		t.Fatal(err)
	}
	constInfo, ok := gen.labelConstants["StatusLabel"]
	if !ok {
		t.Fatal("expected StatusLabel to be in labelConstants")
	}
	if constInfo.value != "status" {
		t.Errorf("expected value 'status', got '%s'", constInfo.value)
	}
}

func TestParseLabelConstantWithMultipleSpaces(t *testing.T) {
	gen := newDocsGenerator("/dev/null")
	gen.state = "LABEL_CONSTANTS"
	if err := gen.processLine(0, `    StatusHumanReadableLabel         = "status"`); err != nil {
		t.Fatal(err)
	}
	constInfo, ok := gen.labelConstants["StatusHumanReadableLabel"]
	if !ok {
		t.Fatal("expected StatusHumanReadableLabel to be in labelConstants")
	}
	if constInfo.value != "status" {
		t.Errorf("expected value 'status', got '%s'", constInfo.value)
	}
}
