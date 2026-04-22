from xfer.est import suggest_shard_count

TIB = 1024**4


def test_below_cap_returns_one_shard():
    suggestion = suggest_shard_count(5 * TIB)
    assert suggestion.num_shards == 1
    assert "below the per-shard cap" in suggestion.reasoning


def test_zero_bytes_returns_one_shard():
    suggestion = suggest_shard_count(0)
    assert suggestion.num_shards == 1


def test_bytes_bounded_by_concurrency_default():
    # 100 TiB, default cap 10 TiB -> 10 shards by bytes.
    # Defaults: array_concurrency=64 -> concurrency bound 256. No core_budget.
    # min(256, max(10, 1)) = 10.
    suggestion = suggest_shard_count(100 * TIB)
    assert suggestion.num_shards == 10


def test_core_budget_tightens_upper_bound():
    # 100 TiB / 10 TiB = 10 shards by bytes.
    # core_budget=40, cpus_per_task=4 -> core bound 10.
    # concurrency bound = 4 * 64 = 256.
    # min(min(256, 10), 10) = 10. Matches the bytes count exactly.
    suggestion = suggest_shard_count(100 * TIB, core_budget=40, cpus_per_task=4)
    assert suggestion.num_shards == 10
    assert "shards_by_cores=10" in suggestion.reasoning


def test_core_budget_caps_below_bytes_requirement():
    # 500 TiB / 10 TiB = 50 shards by bytes.
    # core_budget=16, cpus_per_task=4 -> core bound 4.
    # concurrency bound 256. upper = min(256, 4) = 4.
    # num_shards = max(1, min(4, 50)) = 4.
    suggestion = suggest_shard_count(500 * TIB, core_budget=16, cpus_per_task=4)
    assert suggestion.num_shards == 4


def test_concurrency_caps_below_bytes_requirement():
    # 500 TiB / 10 TiB = 50 shards by bytes. array_concurrency=8 -> bound 32.
    # num_shards = max(1, min(32, 50)) = 32.
    suggestion = suggest_shard_count(500 * TIB, array_concurrency=8)
    assert suggestion.num_shards == 32


def test_custom_max_shard_bytes_tb():
    # max_shard_bytes_tb=1 => cap is 1 TiB.
    # 5 TiB at 1 TiB cap -> 5 shards by bytes. Default concurrency=64 -> upper 256.
    suggestion = suggest_shard_count(5 * TIB, max_shard_bytes_tb=1)
    assert suggestion.num_shards == 5


def test_assumptions_are_echoed():
    suggestion = suggest_shard_count(
        100 * TIB,
        cpus_per_task=8,
        array_concurrency=32,
        core_budget=200,
        max_shard_bytes_tb=20,
    )
    assert suggestion.assumptions == {
        "total_bytes": 100 * TIB,
        "cpus_per_task": 8,
        "array_concurrency": 32,
        "core_budget": 200,
        "max_shard_bytes_tb": 20,
    }
