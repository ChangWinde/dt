#!/usr/bin/env python3
"""Fast dependency-free CUDA allocation probe for the node launcher.

Exit codes:
  0  CUDA context creation and the requested allocation succeeded.
  1  CUDA is present, but the probe failed.
 42  libcuda/the required driver API is unavailable; callers may fall back to
     advisory nvidia-smi checks for projects without a CUDA Python stack.
"""

from __future__ import annotations

import argparse
import ctypes
import sys
from collections.abc import Callable
from typing import Protocol, cast

UNAVAILABLE = 42
DEFAULT_BYTES = 256 * 1024 * 1024


class _CudaFunction(Protocol):
    argtypes: list[object]
    restype: object

    def __call__(self, *args: object) -> int: ...


class _CudaDriver(Protocol):
    cuInit: _CudaFunction
    cuDeviceGet: _CudaFunction
    cuCtxCreate_v2: _CudaFunction
    cuMemAlloc_v2: _CudaFunction
    cuMemFree_v2: _CudaFunction
    cuCtxDestroy_v2: _CudaFunction


def _configure(driver: _CudaDriver) -> None:
    driver.cuInit.argtypes = [ctypes.c_uint]
    driver.cuInit.restype = ctypes.c_int
    driver.cuDeviceGet.argtypes = [ctypes.POINTER(ctypes.c_int), ctypes.c_int]
    driver.cuDeviceGet.restype = ctypes.c_int
    driver.cuCtxCreate_v2.argtypes = [
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_uint,
        ctypes.c_int,
    ]
    driver.cuCtxCreate_v2.restype = ctypes.c_int
    driver.cuMemAlloc_v2.argtypes = [
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.c_size_t,
    ]
    driver.cuMemAlloc_v2.restype = ctypes.c_int
    driver.cuMemFree_v2.argtypes = [ctypes.c_uint64]
    driver.cuMemFree_v2.restype = ctypes.c_int
    driver.cuCtxDestroy_v2.argtypes = [ctypes.c_void_p]
    driver.cuCtxDestroy_v2.restype = ctypes.c_int


def probe(
    allocation_bytes: int = DEFAULT_BYTES,
    *,
    load_library: Callable[[str], object] | None = None,
) -> int:
    """Create a context on visible device 0 and test one real allocation."""
    loader = load_library or ctypes.CDLL
    try:
        driver = cast(_CudaDriver, loader("libcuda.so.1"))
        _configure(driver)
    except (OSError, AttributeError) as exc:
        print(f"CUDA driver probe unavailable: {exc}", file=sys.stderr)
        return UNAVAILABLE

    context = ctypes.c_void_p()
    allocation = ctypes.c_uint64()

    rc = driver.cuInit(0)
    if rc != 0:
        print(f"cuInit failed with CUDA error {rc}", file=sys.stderr)
        return 1

    device = ctypes.c_int()
    rc = driver.cuDeviceGet(ctypes.byref(device), 0)
    if rc != 0:
        print(f"cuDeviceGet failed with CUDA error {rc}", file=sys.stderr)
        return 1

    rc = driver.cuCtxCreate_v2(ctypes.byref(context), 0, device)
    if rc != 0:
        print(f"cuCtxCreate failed with CUDA error {rc}", file=sys.stderr)
        return 1

    result = 0
    try:
        rc = driver.cuMemAlloc_v2(ctypes.byref(allocation), allocation_bytes)
        if rc != 0:
            print(
                f"cuMemAlloc({allocation_bytes}) failed with CUDA error {rc}",
                file=sys.stderr,
            )
            result = 1
        else:
            free_rc = driver.cuMemFree_v2(allocation)
            if free_rc != 0:
                print(
                    f"cuMemFree failed with CUDA error {free_rc}",
                    file=sys.stderr,
                )
                result = 1
    finally:
        destroy_rc = driver.cuCtxDestroy_v2(context)
        if destroy_rc != 0:
            print(
                f"cuCtxDestroy failed with CUDA error {destroy_rc}",
                file=sys.stderr,
            )
            result = 1
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bytes", type=int, default=DEFAULT_BYTES)
    args = parser.parse_args()
    if args.bytes <= 0:
        parser.error("--bytes must be positive")
    return probe(args.bytes)


if __name__ == "__main__":
    raise SystemExit(main())
