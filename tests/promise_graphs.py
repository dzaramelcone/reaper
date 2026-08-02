"""Shared promise graph acts for each transport."""

from reaper.promise import PromiseStore, default_store, durable, set_default_store
from tests.workers import (
    promise_add,
    promise_hold,
    promise_mul,
    promise_square,
    promise_sum,
    promise_value,
)


async def diamond(link: PromiseStore) -> None:
    token = set_default_store(link)
    roots = await durable.gather(promise_value(3))
    root = roots[0].result()
    joined = await durable.join(
        promise_sum,
        promise_add(root, 4),
        promise_mul(root, 5),
    )
    default_store.reset(token)
    assert joined.result() == 22


async def gather(link: PromiseStore) -> None:
    token = set_default_store(link)
    promises = await durable.gather(*(promise_value(value) for value in range(8)))
    values = [promise.result() for promise in promises]
    default_store.reset(token)
    assert values == list(range(8))


async def fanout(link: PromiseStore) -> None:
    token = set_default_store(link)
    promises = await durable.fanout(promise_value, range(10, 16))
    values = [promise.result() for promise in promises]
    default_store.reset(token)
    assert values == [10, 11, 12, 13, 14, 15]


async def join(link: PromiseStore) -> None:
    token = set_default_store(link)
    joined = await durable.join(
        promise_sum,
        promise_value(2),
        promise_value(3),
        promise_value(5),
    )
    default_store.reset(token)
    assert joined.result() == 10


async def first(link: PromiseStore) -> None:
    token = set_default_store(link)
    winner = await durable.first(
        promise_hold(1, 0.08),
        promise_hold(2, 0.01),
        promise_hold(3, 0.05),
    )
    default_store.reset(token)
    assert winner.result() == 2


async def mapreduce(link: PromiseStore) -> None:
    token = set_default_store(link)
    reduced = await durable.mapreduce(
        promise_square,
        promise_sum,
        range(1, 7),
    )
    default_store.reset(token)
    assert reduced.result() == 91


async def dag(link: PromiseStore) -> None:
    token = set_default_store(link)
    roots = await durable.gather(promise_value(2), promise_value(7))
    root = [promise.result() for promise in roots]
    middle = await durable.gather(
        promise_add(root[0], root[1]),
        promise_mul(root[0], root[1]),
        promise_mul(root[1], root[1]),
    )
    values = [promise.result() for promise in middle]
    edge = await durable.gather(promise_add(values[0], values[1]))
    end = await durable.gather(promise_sum([edge[0].result(), values[2]]))
    default_store.reset(token)
    assert end[0].result() == 72


async def dynamic_dag(link: PromiseStore) -> None:
    token = set_default_store(link)
    frontier = [1]
    for width in (2, 3, 2):
        nodes = await durable.gather(
            *(promise_add(parent, branch) for parent in frontier for branch in range(1, width + 1))
        )
        frontier = [promise.result() for promise in nodes]
    end = await durable.gather(promise_sum(frontier))
    default_store.reset(token)
    assert end[0].result() == 72
