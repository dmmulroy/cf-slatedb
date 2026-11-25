import { Array as Arr, Bytes, Cause, Channel, Effect, Pull, Scope, Stream } from "effect";

import { type Row, rowOrder } from "./row.ts";

/** Rows emitted per pull of the merged stream. */
const MERGED_BATCH_SIZE = 256;

/** One input's current chunk and position inside it. */
interface MergeCursor {
  readonly source: number;
  chunk: ReadonlyArray<Row>;
  index: number;
}

const cursorRow = (cursor: MergeCursor): Row => {
  const row = cursor.chunk[cursor.index];
  if (row === undefined) throw new Error("merge cursor is past its chunk");
  return row;
};

/** Heap order: row order, then input position, so equal rows come from the earlier input first. */
const cursorBefore = (left: MergeCursor, right: MergeCursor): boolean => {
  const order = rowOrder(cursorRow(left), cursorRow(right));
  return order < 0 || (order === 0 && left.source < right.source);
};

/** Private binary min-heap of cursors; Effect's `TxPriorityQueue` is transactional and too heavy. */
class CursorHeap {
  private readonly items: Array<MergeCursor> = [];

  get size(): number {
    return this.items.length;
  }

  peek(): MergeCursor | undefined {
    return this.items[0];
  }

  push(cursor: MergeCursor): void {
    this.items.push(cursor);
    let index = this.items.length - 1;
    while (index > 0) {
      const parent = (index - 1) >>> 1;
      const item = this.items[index];
      const parentItem = this.items[parent];
      if (item === undefined || parentItem === undefined || !cursorBefore(item, parentItem)) break;
      this.items[index] = parentItem;
      this.items[parent] = item;
      index = parent;
    }
  }

  pop(): MergeCursor | undefined {
    const top = this.items[0];
    const last = this.items.pop();
    if (top === undefined || last === undefined || this.items.length === 0) return top;
    this.items[0] = last;
    let index = 0;
    for (;;) {
      const left = index * 2 + 1;
      const right = left + 1;
      let smallest = index;
      const leftItem = this.items[left];
      const rightItem = this.items[right];
      const smallestItem = () => this.items[smallest];
      if (leftItem !== undefined && cursorBefore(leftItem, smallestItem() ?? leftItem))
        smallest = left;
      const current = smallestItem();
      if (rightItem !== undefined && current !== undefined && cursorBefore(rightItem, current))
        smallest = right;
      if (smallest === index) break;
      const a = this.items[index];
      const b = this.items[smallest];
      if (a === undefined || b === undefined) break;
      this.items[index] = b;
      this.items[smallest] = a;
      index = smallest;
    }
    return top;
  }
}

/**
 * Merges row streams that are each in {@link rowOrder} (key ascending, sequence descending) into one
 * stream in the same order.
 *
 * **Details**
 *
 * A k-way merge over each input's `Pull` with a private binary heap. Rows with the same key and
 * sequence number are emitted once (from the earliest input): a crash between an L0 upload and the
 * memtable truncation replays such duplicates. Inputs are pulled lazily, one chunk at a time, so a
 * merge never buffers more than one chunk per input.
 *
 * @param sources - Inputs ordered newest first (memtable, then L0 newest to oldest, then sorted runs
 *   by id descending); on an exact duplicate the earliest input wins.
 */
export const mergeRowStreams = <E, R>(
  sources: ReadonlyArray<Stream.Stream<Row, E, R>>,
): Stream.Stream<Row, E, R> =>
  Stream.fromChannel(
    Channel.fromTransform((_upstream, scope) =>
      Effect.gen(function* () {
        const pulls = yield* Effect.forEach(sources, (source) => Stream.toPull(source)).pipe(
          Effect.provideService(Scope.Scope, scope),
        );
        const heap = new CursorHeap();
        let started = false;
        let previous: Row | undefined;

        const refill = (source: number) =>
          Effect.gen(function* () {
            const pull = pulls[source];
            if (pull === undefined) return;
            const chunk = yield* Pull.catchDone(pull, () => Effect.succeed(undefined));
            if (chunk !== undefined) heap.push({ source, chunk, index: 0 });
          });

        const pull: Pull.Pull<Arr.NonEmptyReadonlyArray<Row>, E, void, R> = Effect.gen(
          function* () {
            if (!started) {
              started = true;
              for (let source = 0; source < pulls.length; source++) yield* refill(source);
            }
            const batch: Array<Row> = [];
            while (batch.length < MERGED_BATCH_SIZE) {
              const cursor = heap.pop();
              if (cursor === undefined) break;
              const row = cursorRow(cursor);
              const duplicate =
                previous !== undefined &&
                previous.seq === row.seq &&
                Bytes.Equivalence(previous.key, row.key);
              if (!duplicate) {
                batch.push(row);
                previous = row;
              }
              cursor.index++;
              if (cursor.index < cursor.chunk.length) heap.push(cursor);
              else yield* refill(cursor.source);
            }
            if (Arr.isReadonlyArrayNonEmpty(batch)) return batch;
            return yield* Cause.done();
          },
        );
        return pull;
      }),
    ),
  );
