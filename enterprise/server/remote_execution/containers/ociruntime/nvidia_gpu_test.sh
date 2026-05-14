#!/usr/bin/env bash
set -euo pipefail

: "${TMPDIR:=/tmp}"

nvidia-smi -L

cat > "$TMPDIR"/cuda_probe.cu <<EOF
#include <cstdio>
#include <cuda_runtime.h>

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

  float *d = nullptr;
  err = cudaMalloc(&d, sizeof(float) * 1024);
  if (err != cudaSuccess) {
    std::fprintf(stderr, "cudaMalloc: %s\n", cudaGetErrorString(err));
    return 5;
  }
  cudaFree(d);

  std::printf("cuda_malloc_ok=1\n");
  return 0;
}
EOF

nvcc -O2 "$TMPDIR"/cuda_probe.cu -o "$TMPDIR"/cuda_probe
"$TMPDIR"/cuda_probe
