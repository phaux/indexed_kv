import { getLogger } from "https://deno.land/std@0.201.0/log/mod.ts";
import {
  abortable,
  retry,
  RetryOptions,
} from "https://deno.land/std@0.201.0/async/mod.ts";
import { ulid } from "https://deno.land/x/ulid@v0.3.0/mod.ts";

const logger = () => getLogger("indexed_kv");

const neverSignal = new AbortController().signal;

const MAIN_INDEX = "id" as const;

/**
 * Returns all dotted property paths in object T that hold a value that can be indexed by.
 *
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
 * Defines the shape of values in a {@link Store}.
 *
 * Only used to pass the type parameter to {@link Store} constructor without specifying all of them explicitly.
 * It's just an empty class and does nothing. See https://github.com/microsoft/TypeScript/issues/26242.
 *
 * @template T Type of items stored in the store.
 */
export class Schema<T> {}

/**
 * Options for initializing a {@link Store}.
 */
export interface StoreOptions<T, I> {
  /**
   * Defines the type T of values that will be stored in this store.
   *
   * Doesn't actually do anything, just used to pass the type parameter.
   */
  readonly schema: Schema<T>;
  /**
   * List of dotted property paths in object T that hold a value that can be indexed by.
   */
  readonly indices: readonly I[];
}

/**
 * Options for querying items indexed under a single value.
 *
 * When used, the results will be sorted by ID (creation date).
 */
export interface IndexListValueSelector<T, I extends indexKey<T>> {
  start?: undefined;
  end?: undefined;
  /**
   * Value to be queried.
   *
   * Limits the results to items indexed under this value.
   */
  value: indexValue<T, I>;
  /**
   * Starting ID or date.
   *
   * Limits the results to items created after this date.
   */
  after?: Date | string;
  /**
   * Ending ID or date.
   *
   * Limits the results to items created before this date.
   */
  before?: Date | string;
}

/**
 * Options for querying items indexed under a range of values.
 *
 * When used, the results will be sorted by the index value, then their ID (creation date).
 */
export interface IndexListRangeSelector<T, I extends indexKey<T>> {
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
export interface ListSelector {
  /**
   * Starting ID or date.
   *
   * Limits the results to items created after this date.
   */
  after?: Date | string;
  /**
   * Ending ID or date.
   *
   * Limits the results to items created before this date.
   */
  before?: Date | string;
}

/**
 * Options for operations that return arrays of items.
 */
export interface GetOptions extends Deno.KvListOptions {
  /**
   * Signal to abort collecting the results and return what was collected so far.
   */
  signal?: AbortSignal;
}

/**
 * Store of values of type T.
 *
 * @template T Type of items stored in the store.
 * @template I Union of index paths. Should be inferred automatically.
 */
export class Store<T, I extends indexKey<T>> {
  readonly db: Deno.Kv;
  readonly key: Deno.KvKeyPart;
  readonly indices: readonly I[];

  /**
   * Defines a new store and provides methods for querying and modifying it's contents.
   *
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
   */
  async create(value: T): Promise<Model<T>> {
    const id = ulid();
    await this.db.set([this.key, MAIN_INDEX, id], value);
    logger().debug(
      `Creating ${id}: Created ${[this.key, MAIN_INDEX, id].join("/")}`,
    );
    for (const index of this.indices) {
      const indexValue: Deno.KvKeyPart = index
        .split(".")
        .reduce((value, key) => value[key], value as any);
      await this.db.set([this.key, index, indexValue, id], value);
      logger().debug(
        `Creating ${id}: Created ${
          [this.key, index, indexValue, id].join("/")
        }`,
      );
    }
    return new Model(this, id, value);
  }

  /**
   * Returns a single item from the store by it's ID.
   */
  async getById(id: string): Promise<Model<T> | null> {
    const entry = await this.db.get<T>([this.key, MAIN_INDEX, id]);
    if (entry.versionstamp == null) return null;
    return new Model(this, id, entry.value);
  }

  /**
   * Returns iterator over values based on an index.
   */
  async *listBy<J extends I>(
    index: J,
    selector: IndexListValueSelector<T, J> | IndexListRangeSelector<T, J>,
    options?: Deno.KvListOptions,
  ): AsyncIterableIterator<Model<T>> {
    let kvSelector: Deno.KvListSelector;

    // if single index value was passed
    if (selector.value != null) {
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
    // if index value range was passed
    else if (selector.start != null && selector.end != null) {
      // request items under a range of index values
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
   */
  async getBy<J extends I>(
    index: J,
    selector: IndexListValueSelector<T, J> | IndexListRangeSelector<T, J>,
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
   * Returns iterator over the main by-ID index.
   *
   * It is automatically sorted by creation date.
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
      start = [this.key, MAIN_INDEX, after];
    }

    let end: Deno.KvKey | undefined;
    if (selector?.before != null) {
      const before = selector.before instanceof Date
        ? ulid(selector.before.getTime())
        : selector.before;
      end = [this.key, MAIN_INDEX, before];
    }

    // if both bounds were passed, request specific range
    if (start && end) kvSelector = { start, end };
    // if only one bound was passed, include prefix for whole collection
    else kvSelector = { prefix: [this.key, MAIN_INDEX], start, end };

    for await (const entry of this.db.list<T>(kvSelector, options)) {
      const [_storeKey, _index, id] = entry.key;
      if (typeof id !== "string") continue;
      yield new Model(this, id, entry.value);
    }
  }

  /**
   * Returns array of items from the main by-ID index.
   *
   * It is automatically sorted by creation date.
   */
  async getAll(
    selector?: ListSelector,
    options?: GetOptions,
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
      logger().debug(`Deleting all: Deleted ${[...entry.key].join("/")}`);
    }
  }

  /**
   * Deletes all items from all of the indices except the main by-ID index, then recreates them.
   *
   * Useful when you changed the index definition and want to rebuild it.
   *
   * Warning: This is not an atomic operation.
   * You should make sure this function finishes before the rest of your application is started.
   * If this function fails it throws to make sure your app doesn't start with a broken database (don't catch this error).
   *
   * Example:
   *
   * ```js
   * import { handlers, setup } from "https://deno.land/std/log/mod.ts";
   *
   * setup({
   *   handlers: {
   *     console: new handlers.ConsoleHandler("DEBUG"),
   *   },
   *   loggers: {
   *     indexed_kv: { level: "DEBUG", handlers: ["console"] },
   *   },
   * });
   *
   * // Don't forget await or your app will be running during rebuilding (bad)!
   * await myStore.rebuildIndices();
   *
   * await main();
   * ```
   */
  async rebuildIndices() {
    // remove everything except the main index
    for await (const entry of this.db.list({ prefix: [this.key] })) {
      const [_storeKey, index] = entry.key;
      if (index === MAIN_INDEX) continue;
      await this.db.delete(entry.key);
      logger().info(`Rebuiliding indices: Deleted ${entry.key.join("/")}`);
    }

    // iterate over the new defined indices
    for (const index of this.indices) {
      // iterate over the main index
      for await (
        const entry of this.db.list({ prefix: [this.key, MAIN_INDEX] })
      ) {
        const [_storeKey, _index, id] = entry.key;
        if (typeof id !== "string") continue;
        // get the index value
        const indexValue: Deno.KvKeyPart = index
          .split(".")
          .reduce((value, key) => value[key], entry.value as any);
        // add the item to the index
        await this.db.set([this.key, index, indexValue, id], entry.value);
        logger().info(
          `Rebuiliding indices: Created ${
            [this.key, index, indexValue, id].join("/")
          }`,
        );
      }
    }

    logger().info("Rebuilding indices finished successfully!");
  }

  /**
   * Migrates all items in the store from an old schema to the current one.
   *
   * Use this if you changed the type of an item and want to update all the current items in the store.
   *
   * If your updater function fails to update an item then the migration will be aborted and no changes will be committed.
   *
   * Warning: This is not an atomic operation.
   * You should make sure this function finishes before the rest of your application is started.
   * If this function fails it throws to make sure your app doesn't start with a broken database (don't catch this error).
   *
   * If you changed any field that is also an index field then you should also call {@link Store.rebuildIndices} after this.
   *
   * Example:
   * ```ts
   * import { handlers, setup } from "https://deno.land/std/log/mod.ts";
   * import { Store, Schema } from "https://deno.land/x/indexed_kv/mod.ts";
   *
   * setup({
   *   handlers: {
   *     console: new handlers.ConsoleHandler("DEBUG"),
   *   },
   *   loggers: {
   *     indexed_kv: { level: "DEBUG", handlers: ["console"] },
   *   },
   * });
   *
   * interface UserV1 {
   *   userName: string;
   * }
   *
   * interface UserV2 {
   *   user: { name: string, lastName?: string };
   * }
   *
   * const userStore = new Store(db, "users", { schema: new Schema<UserV2>(), indices: ["user.name"] });
   *
   * // Don't forget await or your app will be running during migrating (bad)!
   * await userStore.migrate((user: UserV1) => ({ user: { name: user.userName } });
   * await userStore.rebuildIndices();
   *
   * await main();
   * ```
   *
   * If somehow the migration fails after the updating phase and during the committing phase
   * then you will end up with some items updated and some not updated.
   * The easiest way to be ready for this is to make a backup of the database file right before migration.
   * You can also still try migrating again but with a updater function that accepts an union of new and old type and acts accordingly.
   */
  async migrate<T1>(
    updater: (value: T1) => T,
  ): Promise<void> {
    const newValues: Record<string, T> = {};
    // iterate over the main index
    for await (
      const entry of this.db.list<T1>({ prefix: [this.key, MAIN_INDEX] })
    ) {
      const [_storeKey, _index, id] = entry.key;
      if (typeof id !== "string") {
        logger().warning(
          `Migrating: Skipped ${entry.key.join("/")} (not a string ID)`,
        );
        continue;
      }
      // try updating and save the value in memory
      try {
        const newValue = updater(entry.value);
        newValues[id] = newValue;
        logger().info(`Migrating: Updated ${entry.key.join("/")}`);
      } catch (err) {
        throw new Error(
          `Migration failed (not committing changes): Failed to update ${
            entry.key.join("/")
          }: ${err}`,
          { cause: err },
        );
      }
    }

    // commit changes
    const totalCount = Object.keys(newValues).length;
    let count = 0;
    for (const [id, value] of Object.entries(newValues)) {
      try {
        await retry(() => this.db.set([this.key, MAIN_INDEX, id], value));
        count++;
        logger().info(
          `Migrating: Committed ${[this.key, MAIN_INDEX, id].join("/")}`,
        );
      } catch (err) {
        throw new Error(
          `Migration failed (${count}/${totalCount} committed): Failed to set ${
            [this.key, MAIN_INDEX, id].join("/")
          }: ${err}`,
          { cause: err },
        );
      }
    }

    logger().info("Migration finished successfully!");
  }
}

/**
 * Single item in a {@link Store}.
 */
export class Model<T> {
  /**
   * The {@link Store} this item belongs to.
   */
  readonly store: Store<T, indexKey<T>>;

  /**
   * Unique ID of the item.
   *
   * You can use it with {@link Store.getById} to retrieve the item again later.
   */
  readonly id: string;

  /**
   * Last retrieved value.
   *
   * It is not automatically synced between multiple model instances representing the same item.
   * It is only updated on creation and when {@link Model.update} is called.
   * You can freely modify it and then call {@link Model.update} to save the changes.
   */
  value: T;

  /**
   * Creates a new model instance.
   * You shouldn't need to call this directly.
   * @internal
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
   *
   * Updater argument should be either a partial object to merge with current value or a function that receives the current value and returns the new value.
   * You can either use the updater argument to provide a new value or modify {@link Model.value} property directly before calling this.
   */
  async attemptUpdate(
    updater?: Partial<T> | ((value: T) => T),
  ): Promise<T | null> {
    // get current main entry
    const oldEntry = await this.store.db.get<T>([
      this.store.key,
      MAIN_INDEX,
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
      .set([this.store.key, MAIN_INDEX, this.id], this.value);
    logger().debug(
      `Updating ${this.id}: Updating ${
        [this.store.key, MAIN_INDEX, this.id].join("/")
      }`,
    );

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
          `Updating ${this.id}: Updating ${
            [this.store.key, index, newIndexKey, this.id].join("/")
          }`,
        );
      } else {
        transaction
          .check(oldIndexEntries[index])
          .delete([this.store.key, index, oldIndexKey, this.id])
          .set([this.store.key, index, newIndexKey, this.id], this.value);
        logger().debug(
          `Updating ${this.id}: Deleting ${
            [this.store.key, index, oldIndexKey, this.id].join("/")
          }`,
        );
        logger().debug(
          `Updating ${this.id}: Creating ${
            [this.store.key, index, newIndexKey, this.id].join("/")
          }`,
        );
      }
    }

    // commit
    const result = await transaction.commit();
    logger().debug(`Updating ${this.id}: Committed`);
    if (!result.ok) {
      throw new Error(
        `Failed to update ${[this.store.key, MAIN_INDEX, this.id].join("/")}`,
      );
    }
    return this.value;
  }

  /**
   * Same as {@link Model.attemptUpdate} but retries on failure.
   *
   * Updates the value in the store's main index and all the defined indices using an atomic transaction.
   *
   * Updater argument should be either a partial object to merge with current value or a function that receives the current value and returns the new value.
   * You can either use the updater argument to provide a new value or modify {@link Model.value} property directly before calling this.
   */
  update(
    updater?: Partial<T> | ((value: T) => T),
    options?: RetryOptions,
  ): Promise<T | null> {
    return retry(() => this.attemptUpdate(updater), options);
  }

  /**
   * Deletes the item from the store's main index and all the defined indices using an atomic transaction.
   *
   * The current {@link Model.value} is not removed and can be still accessed for example to create a new entry.
   */
  async attemptDelete(): Promise<void> {
    // get current main entry
    const entry = await this.store.db.get<T>([
      this.store.key,
      MAIN_INDEX,
      this.id,
    ]);

    // begin transaction
    const transaction = this.store.db.atomic();

    // delete main entry
    transaction
      .check(entry)
      .delete([this.store.key, MAIN_INDEX, this.id]);
    logger().debug(
      `Deleting ${this.id}: Deleting ${
        [this.store.key, MAIN_INDEX, this.id].join("/")
      }`,
    );

    // delete all index entries
    for (const index of this.store.indices) {
      const indexKey: Deno.KvKeyPart = index
        .split(".")
        .reduce((value, key) => value[key], entry.value as any);
      transaction
        .delete([this.store.key, index, indexKey, this.id]);
      logger().debug(
        `Deleting ${this.id}: Deleting ${
          [this.store.key, index, indexKey, this.id].join("/")
        }`,
      );
    }

    // commit
    const result = await transaction.commit();
    logger().debug(`Deleting ${this.id}: Committed`);
    if (!result.ok) {
      throw new Error(
        `Failed to delete ${[this.store.key, MAIN_INDEX, this.id].join("/")}`,
      );
    }
  }

  /**
   * Same as {@link Model.attemptDelete} but retries on failure.
   *
   * Deletes the item from the store's main index and all the defined indices using an atomic transaction.
   *
   * The current {@link Model.value} is not removed and can be still accessed for example to create a new entry.
   */
  delete(options?: RetryOptions): Promise<void> {
    return retry(() => this.attemptDelete(), options);
  }
}
