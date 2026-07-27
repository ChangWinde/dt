import ctypes
import importlib.util

import pytest

from dt.dispatch import PAYLOAD_DIR, _support_files


def _load_module():
    path = PAYLOAD_DIR / "cuda_probe.py"
    spec = importlib.util.spec_from_file_location("dt_cuda_probe_payload", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeFunction:
    def __init__(self, implementation):
        self.implementation = implementation
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.implementation(*args)


class FakeCuda:
    def __init__(
        self,
        *,
        init_rc=0,
        device_rc=0,
        context_rc=0,
        allocation_rc=0,
        free_rc=0,
        destroy_rc=0,
    ):
        self.calls = []
        self.init_rc = init_rc
        self.device_rc = device_rc
        self.context_rc = context_rc
        self.allocation_rc = allocation_rc
        self.free_rc = free_rc
        self.destroy_rc = destroy_rc
        self.cuInit = FakeFunction(self._init)
        self.cuDeviceGet = FakeFunction(self._device_get)
        self.cuCtxCreate_v2 = FakeFunction(self._context_create)
        self.cuMemAlloc_v2 = FakeFunction(self._allocate)
        self.cuMemFree_v2 = FakeFunction(self._free)
        self.cuCtxDestroy_v2 = FakeFunction(self._context_destroy)

    def _init(self, flags):
        self.calls.append(("init", flags))
        return self.init_rc

    def _device_get(self, output, ordinal):
        self.calls.append(("device", ordinal))
        if self.device_rc == 0:
            ctypes.cast(output, ctypes.POINTER(ctypes.c_int))[0] = 7
        return self.device_rc

    def _context_create(self, output, flags, device):
        self.calls.append(("context", flags, device.value))
        if self.context_rc == 0:
            ctypes.cast(output, ctypes.POINTER(ctypes.c_void_p))[0] = 123
        return self.context_rc

    def _allocate(self, output, size):
        self.calls.append(("allocate", size))
        if self.allocation_rc == 0:
            ctypes.cast(output, ctypes.POINTER(ctypes.c_uint64))[0] = 456
        return self.allocation_rc

    def _free(self, allocation):
        self.calls.append(("free", allocation.value))
        return self.free_rc

    def _context_destroy(self, context):
        self.calls.append(("destroy", context.value))
        return self.destroy_rc


def test_cuda_probe_allocates_and_releases_requested_memory():
    module = _load_module()
    driver = FakeCuda()

    assert module.probe(123456, load_library=lambda _name: driver) == 0
    assert driver.calls == [
        ("init", 0),
        ("device", 0),
        ("context", 0, 7),
        ("allocate", 123456),
        ("free", 456),
        ("destroy", 123),
    ]


def test_cuda_probe_failure_still_destroys_context():
    module = _load_module()
    driver = FakeCuda(allocation_rc=2)

    assert module.probe(load_library=lambda _name: driver) == 1
    assert ("free", 456) not in driver.calls
    assert driver.calls[-1] == ("destroy", 123)


def test_cuda_probe_reports_missing_driver_as_unavailable():
    module = _load_module()

    def missing(_name):
        raise OSError("not installed")

    assert module.probe(load_library=missing) == module.UNAVAILABLE


@pytest.mark.parametrize(
    ("driver", "expected_calls", "expected_error"),
    [
        (
            FakeCuda(init_rc=3),
            [("init", 0)],
            "cuInit failed with CUDA error 3",
        ),
        (
            FakeCuda(device_rc=4),
            [("init", 0), ("device", 0)],
            "cuDeviceGet failed with CUDA error 4",
        ),
        (
            FakeCuda(context_rc=5),
            [("init", 0), ("device", 0), ("context", 0, 7)],
            "cuCtxCreate failed with CUDA error 5",
        ),
        (
            FakeCuda(free_rc=6),
            [
                ("init", 0),
                ("device", 0),
                ("context", 0, 7),
                ("allocate", 123456),
                ("free", 456),
                ("destroy", 123),
            ],
            "cuMemFree failed with CUDA error 6",
        ),
        (
            FakeCuda(destroy_rc=7),
            [
                ("init", 0),
                ("device", 0),
                ("context", 0, 7),
                ("allocate", 123456),
                ("free", 456),
                ("destroy", 123),
            ],
            "cuCtxDestroy failed with CUDA error 7",
        ),
    ],
)
def test_cuda_probe_reports_exact_stage_failure_and_cleanup(
    driver, expected_calls, expected_error, capsys
):
    module = _load_module()

    assert module.probe(123456, load_library=lambda _name: driver) == 1
    assert driver.calls == expected_calls
    assert expected_error in capsys.readouterr().err


def test_support_files_ship_cuda_probe():
    files = _support_files(["true"], {"job_id": "j"})
    assert files["cuda_probe.py"] == (PAYLOAD_DIR / "cuda_probe.py").read_text()
