#!/usr/bin/env bash

# Verification script for dev_qa test setup
# Run this to check if your workspace is correctly configured for dev_qa tests

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Dev QA Setup Verification"
echo "=========================================="
echo ""

# Track overall status
ERRORS=0
WARNINGS=0

# Helper functions
error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    ((ERRORS++))
}

warning() {
    echo -e "${YELLOW}⚠ WARNING: $1${NC}"
    ((WARNINGS++))
}

success() {
    echo -e "${GREEN}✓ $1${NC}"
}

info() {
    echo "ℹ $1"
}

# Check 1: Verify we're in a Bazel workspace
echo "Checking Bazel workspace..."
if [[ -f "WORKSPACE" ]] || [[ -f "WORKSPACE.bazel" ]] || [[ -f "MODULE.bazel" ]]; then
    success "Found Bazel workspace file"
else
    error "Not in a Bazel workspace (no WORKSPACE or MODULE.bazel found)"
fi
echo ""

# Check 2: Verify tools directory structure
echo "Checking tools directory..."
if [[ ! -d "tools" ]]; then
    error "tools/ directory not found"
else
    success "Found tools/ directory"

    # Check for required files
    if [[ ! -f "tools/dev_qa_runner.sh" ]]; then
        error "tools/dev_qa_runner.sh not found"
    else
        success "Found tools/dev_qa_runner.sh"

        # Check if executable
        if [[ ! -x "tools/dev_qa_runner.sh" ]]; then
            warning "tools/dev_qa_runner.sh is not executable (should have chmod +x)"
        fi
    fi

    if [[ ! -f "tools/dev_qa.bzl" ]]; then
        error "tools/dev_qa.bzl not found"
    else
        success "Found tools/dev_qa.bzl"
    fi

    if [[ ! -f "tools/BUILD" ]] && [[ ! -f "tools/BUILD.bazel" ]]; then
        error "tools/BUILD or tools/BUILD.bazel not found"
    else
        success "Found tools/BUILD file"

        # Check if it loads dev_qa.bzl
        BUILD_FILE=$(ls tools/BUILD tools/BUILD.bazel 2>/dev/null | head -n1)
        if grep -q "dev_qa.bzl" "${BUILD_FILE}"; then
            success "BUILD file loads dev_qa.bzl"
        else
            error "BUILD file does not load dev_qa.bzl"
        fi

        # Check if bazel_binaries is loaded
        if grep -q "@bazel_binaries" "${BUILD_FILE}"; then
            success "BUILD file references @bazel_binaries"
        else
            warning "BUILD file does not reference @bazel_binaries (tests may be commented out)"
        fi
    fi
fi
echo ""

# Check 3: Verify bazel_binaries configuration
echo "Checking bazel_binaries configuration..."
if [[ -f "MODULE.bazel" ]]; then
    info "Using bzlmod (MODULE.bazel)"

    if grep -q "rules_bazel_integration_test" MODULE.bazel; then
        success "Found rules_bazel_integration_test in MODULE.bazel"
    else
        error "rules_bazel_integration_test not found in MODULE.bazel"
        info "See tools/MODULE.bazel.example for setup instructions"
    fi

    if grep -q "bazel_binaries" MODULE.bazel; then
        success "Found bazel_binaries extension in MODULE.bazel"
    else
        error "bazel_binaries extension not configured in MODULE.bazel"
    fi
elif [[ -f "WORKSPACE" ]] || [[ -f "WORKSPACE.bazel" ]]; then
    info "Using WORKSPACE (legacy mode)"

    WORKSPACE_FILE=$(ls WORKSPACE WORKSPACE.bazel 2>/dev/null | head -n1)

    if grep -q "rules_bazel_integration_test" "${WORKSPACE_FILE}"; then
        success "Found rules_bazel_integration_test in WORKSPACE"
    else
        error "rules_bazel_integration_test not found in WORKSPACE"
        info "See tools/WORKSPACE.example for setup instructions"
    fi

    if grep -q "bazel_binaries" "${WORKSPACE_FILE}"; then
        success "Found bazel_binaries in WORKSPACE"
    else
        error "bazel_binaries not configured in WORKSPACE"
    fi
fi
echo ""

# Check 4: Environment variables
echo "Checking environment variables..."
if [[ -n "${BB_API_KEY:-}" ]]; then
    success "BB_API_KEY is set"
else
    warning "BB_API_KEY is not set"
    info "You'll need to pass --test_env=BB_API_KEY or set it before running tests"
fi

if [[ -n "${QA_ROOT:-}" ]]; then
    info "QA_ROOT is set to: ${QA_ROOT}"
    if [[ ! -d "${QA_ROOT}" ]]; then
        warning "QA_ROOT directory does not exist (will be created on first run)"
    fi
else
    info "QA_ROOT not set (will use default: ~/buildbuddy-qa)"
fi
echo ""

# Check 5: Try to query for test targets
echo "Checking if Bazel can find test targets..."
if command -v bazel &> /dev/null; then
    # Try to query for dev_qa tests
    if bazel query '//tools:dev_qa_all' &>/dev/null; then
        success "Found //tools:dev_qa_all test suite"

        # Try to list all dev_qa tests
        DEV_QA_TESTS=$(bazel query 'kind(script_test, //tools:dev_qa_*)' 2>/dev/null || echo "")
        if [[ -n "${DEV_QA_TESTS}" ]]; then
            success "Found dev_qa test targets:"
            echo "${DEV_QA_TESTS}" | while read -r test; do
                echo "    - ${test}"
            done
        else
            warning "No dev_qa test targets found (they may be commented out in tools/BUILD)"
        fi
    else
        error "Could not find //tools:dev_qa_all"
        info "Make sure the test targets are uncommented in tools/BUILD"
    fi
else
    warning "bazel command not found, skipping target query"
fi
echo ""

# Check 6: Verify git is available (needed for cloning repos)
echo "Checking dependencies..."
if command -v git &> /dev/null; then
    success "git is available"
else
    error "git command not found (required for cloning test repositories)"
fi

if command -v uuidgen &> /dev/null; then
    success "uuidgen is available"
elif [[ -f /proc/sys/kernel/random/uuid ]]; then
    success "UUID generation available via /proc/sys/kernel/random/uuid"
else
    warning "No UUID generation method found (invocation IDs may fall back to timestamp)"
fi
echo ""

# Summary
echo "=========================================="
echo "Summary"
echo "=========================================="

if [[ ${ERRORS} -eq 0 ]] && [[ ${WARNINGS} -eq 0 ]]; then
    echo -e "${GREEN}✓ All checks passed!${NC}"
    echo ""
    echo "You can now run the dev QA tests:"
    echo "  bazel test //tools:dev_qa_all --test_env=BB_API_KEY"
    echo ""
    echo "Or test a specific repository:"
    echo "  bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY"
    exit 0
elif [[ ${ERRORS} -eq 0 ]]; then
    echo -e "${YELLOW}⚠ ${WARNINGS} warning(s) found${NC}"
    echo ""
    echo "The setup should work, but you may want to address the warnings."
    echo ""
    echo "Try running a test:"
    echo "  bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY"
    exit 0
else
    echo -e "${RED}✗ ${ERRORS} error(s) found${NC}"
    if [[ ${WARNINGS} -gt 0 ]]; then
        echo -e "${YELLOW}⚠ ${WARNINGS} warning(s) found${NC}"
    fi
    echo ""
    echo "Please fix the errors above before running dev QA tests."
    echo ""
    echo "See the migration guide for setup instructions:"
    echo "  tools/MIGRATION_GUIDE.md"
    echo ""
    echo "Or check the example configs:"
    echo "  tools/MODULE.bazel.example (for bzlmod)"
    echo "  tools/WORKSPACE.example (for legacy WORKSPACE)"
    exit 1
fi
