#!/usr/bin/env bash
set -euo pipefail

: "${TMPDIR:=/tmp}"

# Verify that the container can discover the GPUs exposed by the runtime.
nvidia-smi -L

# Build a small CUDA probe that grows its allocation by 64 MiB every two
# seconds, then releases everything long enough for polling to observe zero.
cat > "$TMPDIR"/cuda_probe.cu <<EOF
#include <cstdio>
#include <cuda_runtime.h>
#include <unistd.h>

int main() {
  int n = 0;
  cudaError_t err = cudaGetDeviceCount(&n);
  if (err != cudaSuccess) {
    std::fprintf(stderr, "cudaGetDeviceCount: %s\n", cudaGetErrorString(err));
    return 2;
  }
  std::printf("device_count=%d\n", n);
  if (n < 1) {
    std::fprintf(stderr, "no CUDA devices reported\n");
    return 3;
  }

  err = cudaSetDevice(0);
  if (err != cudaSuccess) {
    std::fprintf(stderr, "cudaSetDevice: %s\n", cudaGetErrorString(err));
    return 4;
  }

  constexpr size_t allocation_bytes = 64 * 1024 * 1024;
  constexpr int allocation_count = 5;
  void *allocations[allocation_count] = {};
  for (int i = 0; i < allocation_count; i++) {
    err = cudaMalloc(&allocations[i], allocation_bytes);
    if (err != cudaSuccess) {
      std::fprintf(stderr, "cudaMalloc: %s\n", cudaGetErrorString(err));
      return 5;
    }
    std::printf("cuda_allocated_bytes=%zu\n", allocation_bytes * (i + 1));
    std::fflush(stdout);
    sleep(2);
  }
  for (int i = 0; i < allocation_count; i++) {
    err = cudaFree(allocations[i]);
    if (err != cudaSuccess) {
      std::fprintf(stderr, "cudaFree: %s\n", cudaGetErrorString(err));
      return 6;
    }
  }
  err = cudaDeviceReset();
  if (err != cudaSuccess) {
    std::fprintf(stderr, "cudaDeviceReset: %s\n", cudaGetErrorString(err));
    return 7;
  }
  std::printf("cuda_freed_all=1\n");
  std::fflush(stdout);
  sleep(2);

  std::printf("cuda_probe_done=1\n");
  return 0;
}
EOF

# Compile and run the probe using the CUDA toolkit from the test image.
nvcc -O2 "$TMPDIR"/cuda_probe.cu -o "$TMPDIR"/cuda_probe
"$TMPDIR"/cuda_probe
