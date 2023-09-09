import { getLogger } from "https://deno.land/std@0.201.0/log/mod.ts";
import {
  abortable,
  retry,
  RetryOptions,
} from "https://deno.land/std@0.201.0/async/mod.ts";
import { ulid } from "https://deno.land/x/ulid@v0.3.0/mod.ts";

const logger = () => getLogger("indexed_kv");

const neverSignal = new AbortController().signal;

/**
 * Returns all dotted property paths in object T that hold a value that can be indexed by.
 * Value to be indexed must be a {@link Deno.KvKeyPart}.
 */
export type indexKey<T> = {
  [K in keyof T]:
    // for every key in T
    K extends string
      // the key is a string
      ? (
        // value at key is a valid Kv key value? return that key
        T[K] extends Deno.KvKeyPart ? K
          // value at key is an array? skip that key
          : T[K] extends readonly unknown[] ? never
          // value at key is an object? return all dotted paths in that object
          : T[K] extends object ? `${K}.${indexKey<T[K]>}`
          // none of the above
          : never
      )
      // K is not a string
      : never;
}[keyof T];

/**
 * For given object T and dotted property path I returns the type of value at that property.
 */
export type indexValue<T, I extends indexKey<T>> =
  & (
    I extends `${infer K}.${infer Rest}`
      // we received a key path
      ? (
        K extends keyof T
          // first path segment is a key of T
          ? (
            Rest extends indexKey<T[K]>
              // the rest of the path is a valid index key of T[K]
              ? indexValue<T[K], Rest>
              // the rest of the path is not a valid index key of T[K]
              : never
          )
          // first path segment is not a key of T
          : never
      )
      // we received a single key
      : (
        I extends keyof T
          // the key is a key of T
          ? T[I]
          // the key is not a key of T
          : never
      )
  )
  & Deno.KvKeyPart;

/**
 * Only used to pass the type parameter to {@link Store} constructor without specifying all of them explicitly.
 * It's just an empty class and does nothing. See https://github.com/microsoft/TypeScript/issues/26242
 */
export class Schema<T> {}

/**
 * Options for initializing a {@link Store}.
 */
interface StoreOptions<T, I> {
  /**
   * Defines the type T of values that will be stored in this store.
   * Doesn't actually do anything, just used to pass the type parameter.
   */
  readonly schema: Schema<T>;
  /**
   * List of dotted property paths in object T that hold a value that can be indexed by.
   */
  readonly indices: readonly I[];
}

/**
 * Options for querying by index.
 * Passing a value directly is shortcut for `{ value: value }`.
 */
type IndexListSelector<T, I extends indexKey<T>> =
  | indexValue<T, I>
  | IndexListValueSelector<T, I>
  | IndexListRangeSelector<T, I>;

/**
 * Options for querying items indexed under a single value.
 * When used, the results will be sorted by id (creation date).
 */
interface IndexListValueSelector<T, I extends indexKey<T>> {
  start?: undefined;
  end?: undefined;
  /**
   * Value to be queried.
   * Limits the results to items indexed under this value.
   */
  value: indexValue<T, I>;
  /**
   * Starting id or date.
   * Limits the results to items created after this date.
   * See {@link Deno.KvListSelector}.
   */
  after?: Date | string;
  /**
   * Ending id or date.
   * Limits the results to items created before this date.
   * See {@link Deno.KvListSelector}.
   */
  before?: Date | string;
}

/**
 * Options for querying items indexed under a range of values.
 * When used, the results will be sorted by the index value, then their id (creation date).
 */
interface IndexListRangeSelector<T, I extends indexKey<T>> {
  /**
   * Starting index value.
   */
  start: indexValue<T, I>;
  /**
   * Ending index value.
   */
  end: indexValue<T, I>;
  value?: undefined;
  after?: undefined;
  before?: undefined;
}

/**
 * Options for querying all items in a collection.
 */
type ListSelector = {
  /**
   * Starting id or date.
   * Limits the results to items created after this date.
   */
  after?: Date | string;
  /**
   * Ending id or date.
   * Limits the results to items created before this date.
   */
  before?: Date | string;
};

/**
 * Store of values of type T.
 */
export class Store<T, I extends indexKey<T>> {
  readonly db: Deno.Kv;
  readonly key: Deno.KvKeyPart;
  readonly indices: readonly I[];

  /**
   * Defines a new store and provides methods for querying and modifying it's contents.
   * Doesn't actually create anything in the database until the first item is created.
   *
   * @param db Kv instance.
   * @param key Key in Kv database under which this store will store its data.
   * @param options Other options.
   */
  constructor(db: Deno.Kv, key: Deno.KvKeyPart, options: StoreOptions<T, I>) {
    this.db = db;
    this.key = key;
    this.indices = options.indices;
  }

  /**
   * Creates a new item in the store.
   *
   * Adds item to the main index and all the defined indices.
   * Automatically assigns a new id to the item using {@link ulid}.
   *
   * @param value Value to be stored.
   * @returns Model instance for the newly created item.
   */
  async create(value: T): Promise<Model<T>> {
    const id = ulid();
    await this.db.set([this.key, "id", id], value);
    logger().debug(["created", this.key, "id", id].join(" "));
    for (const index of this.indices) {
      const indexValue: Deno.KvKeyPart = index
        .split(".")
        .reduce((value, key) => value[key], value as any);
      await this.db.set([this.key, index, indexValue, id], value);
      logger().debug(["created", this.key, index, indexValue, id].join(" "));
    }
    return new Model(this, id, value);
  }

  /**
   * Returns a single item from the store by it's id.
   * @param id
   * @returns
   */
  async getById(id: string): Promise<Model<T> | null> {
    const entry = await this.db.get<T>([this.key, "id", id]);
    if (entry.versionstamp == null) return null;
    return new Model(this, id, entry.value);
  }

  /**
   * Returns iterator over values based on an index.
   *
   * @param index Dotted property path in object T.
   * @param selector Which values to find.
   * @param options Options to pass to {@link Deno.Kv.list}.
   * @yields Model instances for the found items.
   */
  async *listBy<J extends I>(
    index: J,
    selector: IndexListSelector<T, J>,
    options?: Deno.KvListOptions,
  ): AsyncIterableIterator<Model<T>> {
    let kvSelector: Deno.KvListSelector;
    // if index value was passed directly
    if (typeof selector !== "object" || ArrayBuffer.isView(selector)) {
      // request all items under index value
      kvSelector = { prefix: [this.key, index, selector] };
    } //
    // if index value was passed as object with before / after dates
    else if (selector.value != null) {
      // request items under index value limited by date
      let start: Deno.KvKey | undefined;
      if (selector.after != null) {
        const after = selector.after instanceof Date
          ? ulid(selector.after.getTime())
          : selector.after;
        start = [this.key, index, selector.value, after];
      }

      let end: Deno.KvKey | undefined;
      if (selector.before != null) {
        const before = selector.before instanceof Date
          ? ulid(selector.before.getTime())
          : selector.before;
        end = [this.key, index, selector.value, before];
      }

      // if both bounds were passed, request specific range
      if (start && end) kvSelector = { start, end };
      // if only one bound was passed, include prefix for whole index
      else {kvSelector = {
          prefix: [this.key, index, selector.value],
          start,
          end,
        };}
    } //
    // index value range start / end was passed as object
    else if (selector.start != null && selector.end != null) {
      // request items under multiple index values
      kvSelector = {
        start: [this.key, index, selector.start],
        end: [this.key, index, selector.end],
      };
    } // invalid selector
    else {
      throw new TypeError("Invalid selector");
    }

    for await (const entry of this.db.list<T>(kvSelector, options)) {
      const [_storeKey, _index, _indexValue, id] = entry.key;
      if (typeof id !== "string") continue;
      yield new Model(this, id, entry.value);
    }
  }

  /**
   * Returns array of values based on an index.
   *
   * @param index Dotted property path in object T.
   * @param selector Which values to find.
   * @param options Options to pass to {@link Deno.Kv.list}.
   * @returns Array of Model instances for the found items.
   */
  async getBy<J extends I>(
    index: J,
    selector: IndexListSelector<T, J>,
    options?: Deno.KvListOptions & { signal?: AbortSignal },
  ): Promise<Array<Model<T>>> {
    const items: Model<T>[] = [];
    const signal = options?.signal ?? neverSignal;
    try {
      for await (
        const item of abortable(this.listBy(index, selector, options), signal)
      ) {
        items.push(item);
      }
    } catch (error) {
      if (!options?.signal?.aborted) throw error;
    }
    return items;
  }

  /**
   * Returns iterator over the main by-id index.
   * It is automatically sorted by creation date.
   *
   * @param selector Which ids to find.
   * @param options Options to pass to {@link Deno.Kv.list}.
   * @yields Model instances for the found items.
   */
  async *listAll(
    selector?: ListSelector,
    options?: Deno.KvListOptions,
  ): AsyncIterableIterator<Model<T>> {
    let kvSelector: Deno.KvListSelector;

    let start: Deno.KvKey | undefined;
    if (selector?.after != null) {
      const after = selector.after instanceof Date
        ? ulid(selector.after.getTime())
        : selector.after;
      start = [this.key, "id", after];
    }

    let end: Deno.KvKey | undefined;
    if (selector?.before != null) {
      const before = selector.before instanceof Date
        ? ulid(selector.before.getTime())
        : selector.before;
      end = [this.key, "id", before];
    }

    // if both bounds were passed, request specific range
    if (start && end) kvSelector = { start, end };
    // if only one bound was passed, include prefix for whole collection
    else kvSelector = { prefix: [this.key, "id"], start, end };

    for await (const entry of this.db.list<T>(kvSelector, options)) {
      const [_storeKey, _index, id] = entry.key;
      if (typeof id !== "string") continue;
      yield new Model(this, id, entry.value);
    }
  }

  /**
   * Returns array of items from the main by-id index.
   * It is automatically sorted by creation date.
   *
   * @param selector Which ids to find.
   * @param options Options to pass to {@link Deno.Kv.list}.
   * @returns Array of Model instances for the found items.
   */
  async getAll(
    selector?: ListSelector,
    options?: Deno.KvListOptions & { signal?: AbortSignal },
  ): Promise<Array<Model<T>>> {
    const items: Model<T>[] = [];
    const signal = options?.signal ?? neverSignal;
    try {
      for await (
        const item of abortable(this.listAll(selector, options), signal)
      ) {
        items.push(item);
      }
    } catch (error) {
      if (!options?.signal?.aborted) throw error;
    }
    return items;
  }

  /**
   * Deletes all items from the store.
   */
  async deleteAll(): Promise<void> {
    for await (const entry of this.db.list({ prefix: [this.key] })) {
      await this.db.delete(entry.key);
      logger().debug(["deleted", ...entry.key].join(" "));
    }
  }
}

/**
 * Single item in a {@link Store}.
 */
export class Model<T> {
  readonly store: Store<T, indexKey<T>>;
  readonly id: string;
  /**
   * Last retrieved value.
   * It is not automatically synced between multiple model instances representing the same item.
   * It is only updated on creation and when {@link Model.update} is called.
   * You can freely modify it and then call {@link Model.update} to save the changes.
   */
  value: T;

  /**
   * @internal
   * Creates a new model instance.
   * You shouldn't need to call this directly.
   */
  constructor(
    store: Store<T, indexKey<T>>,
    id: string,
    value: T,
  ) {
    this.store = store;
    this.id = id;
    this.value = value;
  }

  /**
   * Updates the value in the store's main index and all the defined indices using an atomic transaction.
   * You can either use the updater argument to provide a new value or modify {@link Model.value} property directly.
   *
   * @param updater Either a partial value to be merged with the current value or a function that receives the current value and returns the new value.
   * @returns New value.
   */
  async attemptUpdate(
    updater?: Partial<T> | ((value: T) => T),
  ): Promise<T | null> {
    // get current main entry
    const oldEntry = await this.store.db.get<T>([
      this.store.key,
      "id",
      this.id,
    ]);

    // get all current index entries
    const oldIndexEntries: Record<string, Deno.KvEntryMaybe<T>> = {};
    for (const index of this.store.indices) {
      const indexKey: Deno.KvKeyPart = index
        .split(".")
        .reduce((value, key) => value[key], oldEntry.value as any);
      oldIndexEntries[index] = await this.store.db.get<T>([
        this.store.key,
        index,
        indexKey,
        this.id,
      ]);
    }

    // compute new value
    if (typeof updater === "function") {
      this.value = updater(this.value);
    } else {
      this.value = { ...this.value, ...updater };
    }

    // begin transaction
    const transaction = this.store.db.atomic();

    // set the main entry
    transaction
      .check(oldEntry)
      .set([this.store.key, "id", this.id], this.value);
    logger().debug(["updated", this.store.key, "id", this.id].join(" "));

    // update all index entries
    for (const index of this.store.indices) {
      const oldIndexKey: Deno.KvKeyPart = index
        .split(".")
        .reduce(
          (value, key) => value[key],
          oldIndexEntries[index].value as any,
        );
      const newIndexKey: Deno.KvKeyPart = index
        .split(".")
        .reduce((value, key) => value[key], this.value as any);
      if (newIndexKey === oldIndexKey) {
        transaction
          .check(oldIndexEntries[index])
          .set([this.store.key, index, newIndexKey, this.id], this.value);
        logger().debug(
          ["updated", this.store.key, index, newIndexKey, this.id].join(" "),
        );
      } else {
        transaction
          .check(oldIndexEntries[index])
          .delete([this.store.key, index, oldIndexKey, this.id])
          .set([this.store.key, index, newIndexKey, this.id], this.value);
        logger().debug(
          ["deleted", this.store.key, index, oldIndexKey, this.id].join(" "),
        );
        logger().debug(
          ["created", this.store.key, index, newIndexKey, this.id].join(" "),
        );
      }
    }

    // commit
    const result = await transaction.commit();
    if (!result.ok) {
      throw new Error(`Failed to update ${this.store.key} ${this.id}`);
    }
    return this.value;
  }

  /**
   * Same as {@link Model.attemptUpdate} but retries on failure.
   */
  update(
    updater: Partial<T> | ((value: T) => T),
    options?: RetryOptions,
  ): Promise<T | null> {
    return retry(() => this.attemptUpdate(updater), options);
  }

  /**
   * Deletes the item from the store's main index and all the defined indices using an atomic transaction.
   * The saved {@link Model.value} is not updated.
   * You can call `item.delete()` and then `itemStore.create(item.value)` to update the id of the item to the current date.
   */
  async attemptDelete(): Promise<void> {
    // get current main entry
    const entry = await this.store.db.get<T>([this.store.key, "id", this.id]);

    // begin transaction
    const transaction = this.store.db.atomic();

    // delete main entry
    transaction
      .check(entry)
      .delete([this.store.key, "id", this.id]);
    logger().debug(["deleted", this.store.key, "id", this.id].join(" "));

    // delete all index entries
    for (const index of this.store.indices) {
      const indexKey: Deno.KvKeyPart = index
        .split(".")
        .reduce((value, key) => value[key], entry.value as any);
      transaction
        .delete([this.store.key, index, indexKey, this.id]);
      logger().debug(
        ["deleted", this.store.key, index, indexKey, this.id].join(" "),
      );
    }

    // commit
    const result = await transaction.commit();
    if (!result.ok) {
      throw new Error(`Failed to delete ${this.store.key} ${this.id}`);
    }
  }

  /**
   * Same as {@link Model.attemptDelete} but retries on failure.
   */
  delete(options?: RetryOptions): Promise<void> {
    return retry(() => this.attemptDelete(), options);
  }
}
