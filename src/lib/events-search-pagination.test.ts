import assert from "node:assert/strict";
import test from "node:test";
import {
  newestFirstEventSearchChunks,
  planEventSearchPageFetches,
  splitEventSearchDateRange,
} from "./events-search-pagination";

test("splitEventSearchDateRange keeps short ranges as one chunk", () => {
  const chunks = splitEventSearchDateRange(
    "2026-01-01T00:00:00.000Z",
    "2026-01-07T00:00:00.000Z"
  );

  assert.deepEqual(chunks, [
    {
      startDate: "2026-01-01T00:00:00.000Z",
      endDate: "2026-01-07T00:00:00.000Z",
    },
  ]);
});

test("splitEventSearchDateRange splits long ranges into non-overlapping chunks", () => {
  const chunks = splitEventSearchDateRange(
    "2026-01-01T00:00:00.000Z",
    "2026-03-10T00:00:00.000Z"
  );

  assert.equal(chunks.length, 3);
  assert.equal(chunks[0].startDate, "2026-01-01T00:00:00.000Z");
  assert.equal(chunks.at(-1)?.endDate, "2026-03-10T00:00:00.000Z");
  assert.ok(
    new Date(chunks[1].startDate).getTime() >
      new Date(chunks[0].endDate).getTime()
  );
});

test("newestFirstEventSearchChunks returns newest chunk first", () => {
  const chunks = splitEventSearchDateRange(
    "2026-01-01T00:00:00.000Z",
    "2026-03-10T00:00:00.000Z"
  );

  const newestFirst = newestFirstEventSearchChunks(chunks);

  assert.equal(newestFirst[0].endDate, "2026-03-10T00:00:00.000Z");
  assert.equal(newestFirst.at(-1)?.startDate, "2026-01-01T00:00:00.000Z");
});

test("planEventSearchPageFetches skips older chunks when offset is in newest chunk", () => {
  const fetches = planEventSearchPageFetches(
    [
      { startDate: "new-start", endDate: "new-end", total: 100 },
      { startDate: "old-start", endDate: "old-end", total: 100 },
    ],
    25,
    50
  );

  assert.deepEqual(fetches, [
    {
      startDate: "new-start",
      endDate: "new-end",
      offset: 25,
      limit: 50,
    },
  ]);
});

test("planEventSearchPageFetches spans chunks when a page crosses a boundary", () => {
  const fetches = planEventSearchPageFetches(
    [
      { startDate: "new-start", endDate: "new-end", total: 30 },
      { startDate: "mid-start", endDate: "mid-end", total: 40 },
      { startDate: "old-start", endDate: "old-end", total: 40 },
    ],
    20,
    50
  );

  assert.deepEqual(fetches, [
    {
      startDate: "new-start",
      endDate: "new-end",
      offset: 20,
      limit: 10,
    },
    {
      startDate: "mid-start",
      endDate: "mid-end",
      offset: 0,
      limit: 40,
    },
  ]);
});

test("planEventSearchPageFetches skips whole chunks before page start", () => {
  const fetches = planEventSearchPageFetches(
    [
      { startDate: "new-start", endDate: "new-end", total: 30 },
      { startDate: "mid-start", endDate: "mid-end", total: 40 },
      { startDate: "old-start", endDate: "old-end", total: 40 },
    ],
    70,
    25
  );

  assert.deepEqual(fetches, [
    {
      startDate: "old-start",
      endDate: "old-end",
      offset: 0,
      limit: 25,
    },
  ]);
});
