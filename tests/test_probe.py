from dt.probe import SEP, parse_probe_output

SAMPLE = f"""0, GPU-aaa, 3, 81920, 0
1, GPU-bbb, 76000, 81920, 98
2, GPU-ccc, 800, 81920, 0
{SEP}
GPU-bbb, 12345
GPU-bbb, 12346
"""


def test_free_iff_no_procs_and_low_mem():
    gpus = parse_probe_output(SAMPLE, mem_threshold_mib=500)
    by_idx = {g.index: g for g in gpus}
    assert by_idx[0].free                       # idle
    assert not by_idx[1].free                   # busy: procs + memory
    assert by_idx[1].procs == 2
    assert not by_idx[2].free                   # zombie ctx: mem>threshold, no procs


def test_threshold_boundary():
    gpus = parse_probe_output(SAMPLE, mem_threshold_mib=1000)
    by_idx = {g.index: g for g in gpus}
    assert by_idx[2].free                       # 800 MiB < 1000 threshold


def test_empty_apps_section():
    text = f"0, GPU-x, 0, 81920, 0\n{SEP}\n"
    gpus = parse_probe_output(text, 500)
    assert len(gpus) == 1 and gpus[0].free
