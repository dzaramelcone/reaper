"""Check typed Reaper daemon flags."""

import pytest
from pydantic import ValidationError

from reaper.cli import ReaperCLI
from reaper.settings import PoolConfig, PoolKind

DSN = "postgresql://reaper:reaper@127.0.0.1:55433/reaper"


def test_pool_flags_declare_mixed_topics() -> None:
    settings = ReaperCLI(
        _cli_parse_args=[
            "--postgres-dsn",
            DSN,
            "--pool",
            "math:4",
            "--pool",
            "maintenance:1",
            "--pool",
            "cat:3",
        ],
    )

    assert [pool.skeletons for pool in settings.pools] == [4, 1, 3]
    assert [pool.topic for pool in settings.pools] == ["math", None, "cat"]
    assert [pool.kind for pool in settings.pools] == [
        PoolKind.TASK,
        PoolKind.MAINTENANCE,
        PoolKind.TASK,
    ]


def test_pool_topic_is_optional() -> None:
    settings = ReaperCLI(
        _cli_parse_args=[
            "--postgres-dsn",
            DSN,
            "--pool",
            "4",
        ],
    )

    assert settings.pools[0].skeletons == 4
    assert settings.pools[0].topic is None


def test_pool_flag_rejects_bad_shapes() -> None:
    with pytest.raises(ValidationError):
        ReaperCLI(
            _cli_parse_args=[
                "--postgres-dsn",
                DSN,
                "--pool",
                "math",
            ],
        )


def test_horde_resource_limits_are_validated() -> None:
    with pytest.raises(ValidationError, match="less than or equal to 64"):
        ReaperCLI(
            postgres_dsn=DSN,
            pools=[PoolConfig(skeletons=65)],
        )

    with pytest.raises(ValidationError, match="at most 256 skeletons"):
        ReaperCLI(
            postgres_dsn=DSN,
            pools=[PoolConfig(skeletons=64)] * 5,
        )

    with pytest.raises(ValidationError, match="retry base"):
        ReaperCLI(
            postgres_dsn=DSN,
            service_retry_base=10,
            service_retry_max=1,
            pools=[PoolConfig(skeletons=1)],
        )
