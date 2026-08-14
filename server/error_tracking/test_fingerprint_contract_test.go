package error_tracking

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed testdata/test_fingerprint_contract.json
var testFingerprintContractJSON []byte

type testFingerprintContract struct {
	Version int                           `json:"version"`
	Cases   []testFingerprintContractCase `json:"cases"`
}

type testFingerprintContractCase struct {
	ID            string  `json:"id"`
	Source        string  `json:"source"`
	Target        string  `json:"target"`
	Suite         string  `json:"suite"`
	Class         string  `json:"class"`
	Test          string  `json:"test"`
	Kind          string  `json:"kind"`
	Type          string  `json:"type"`
	Message       string  `json:"message"`
	Body          string  `json:"body"`
	FinalStatus   string  `json:"final_status"`
	ExpectedIssue *string `json:"expected_issue"`
}

func loadTestFingerprintContract(t testing.TB) *testFingerprintContract {
	t.Helper()
	contract := &testFingerprintContract{}
	require.NoError(t, json.Unmarshal(testFingerprintContractJSON, contract))
	require.Equal(t, 5, contract.Version)
	require.NotEmpty(t, contract.Cases)
	ids := make(map[string]struct{}, len(contract.Cases))
	for _, testCase := range contract.Cases {
		require.NotEmpty(t, testCase.ID)
		_, duplicate := ids[testCase.ID]
		require.Falsef(t, duplicate, "duplicate contract case %q", testCase.ID)
		ids[testCase.ID] = struct{}{}
	}
	return contract
}

// scoreTestFingerprintContract compares an implementation's case-to-fingerprint
// mapping with the ground truth. It reports false merges and false splits
// separately so reducing the number of groups cannot hide a loss of precision.
func scoreTestFingerprintContract(contract *testFingerprintContract, actual map[string]string) (falseMerges, falseSplits []string) {
	expectedByActual := make(map[string]map[string]struct{})
	actualByExpected := make(map[string]map[string]struct{})
	for _, testCase := range contract.Cases {
		fingerprint := actual[testCase.ID]
		if testCase.ExpectedIssue == nil {
			if fingerprint != "" {
				falseMerges = append(falseMerges, fmt.Sprintf("suppressed case %s produced %s", testCase.ID, fingerprint))
			}
			continue
		}
		if fingerprint == "" {
			falseSplits = append(falseSplits, fmt.Sprintf("case %s produced no fingerprint", testCase.ID))
			continue
		}
		expected := *testCase.ExpectedIssue
		if expectedByActual[fingerprint] == nil {
			expectedByActual[fingerprint] = make(map[string]struct{})
		}
		expectedByActual[fingerprint][expected] = struct{}{}
		if actualByExpected[expected] == nil {
			actualByExpected[expected] = make(map[string]struct{})
		}
		actualByExpected[expected][fingerprint] = struct{}{}
	}
	for fingerprint, expected := range expectedByActual {
		if len(expected) > 1 {
			falseMerges = append(falseMerges, fmt.Sprintf("fingerprint %s merged %d expected issues", fingerprint, len(expected)))
		}
	}
	for expected, fingerprints := range actualByExpected {
		if len(fingerprints) > 1 {
			falseSplits = append(falseSplits, fmt.Sprintf("expected issue %s split into %d fingerprints", expected, len(fingerprints)))
		}
	}
	return falseMerges, falseSplits
}

func TestTestFingerprintContractScorerDetectsFalseMergesAndSplits(t *testing.T) {
	contract := loadTestFingerprintContract(t)

	allMerged := make(map[string]string, len(contract.Cases))
	allSplit := make(map[string]string, len(contract.Cases))
	for _, testCase := range contract.Cases {
		if testCase.ExpectedIssue == nil {
			continue
		}
		allMerged[testCase.ID] = "one-fingerprint"
		allSplit[testCase.ID] = "fingerprint-for-" + testCase.ID
	}
	falseMerges, _ := scoreTestFingerprintContract(contract, allMerged)
	require.NotEmpty(t, falseMerges)
	_, falseSplits := scoreTestFingerprintContract(contract, allSplit)
	require.NotEmpty(t, falseSplits)
}

func TestCurrentTestLogFingerprintContractBaseline(t *testing.T) {
	contract := loadTestFingerprintContract(t)
	actual := make(map[string]string, len(contract.Cases))
	for _, testCase := range contract.Cases {
		message := testCase.Body
		if message == "" {
			message = testCase.Message
		}
		// This intentionally models the current behavior: a failed TestResult is
		// reduced to the generic test error type plus one selected log signature,
		// without target/testcase identity or final-summary reconciliation.
		actual[testCase.ID] = occurrenceFingerprint("test/FAILED", "", message)
	}
	falseMerges, falseSplits := scoreTestFingerprintContract(contract, actual)
	require.NotEmpty(t, falseMerges, "the baseline should expose at least one unsafe merge")
	require.NotEmpty(t, falseSplits, "the baseline should expose at least one unstable split")
	t.Logf("current test.log baseline: %d false merges, %d false splits", len(falseMerges), len(falseSplits))
}

func TestStructuredTestFingerprintContract(t *testing.T) {
	contract := loadTestFingerprintContract(t)
	actual := make(map[string]string, len(contract.Cases))
	for _, testCase := range contract.Cases {
		if testCase.FinalStatus == "PASSED" || testCase.FinalStatus == "FLAKY" {
			continue
		}
		if testCase.Source == "fallback" {
			actual[testCase.ID], _ = TestFallbackFingerprint(testCase.Target, testCase.FinalStatus, testCase.Message)
			continue
		}
		actual[testCase.ID], _ = TestFailureFingerprint(TestFailure{
			TargetLabel: testCase.Target,
			SuiteName:   testCase.Suite,
			ClassName:   testCase.Class,
			TestName:    testCase.Test,
			Kind:        testCase.Kind,
			Type:        testCase.Type,
			Message:     testCase.Message,
			Body:        testCase.Body,
		})
	}
	falseMerges, falseSplits := scoreTestFingerprintContract(contract, actual)
	require.Empty(t, falseMerges)
	require.Empty(t, falseSplits)
}
