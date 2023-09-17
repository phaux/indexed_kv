import { getLogger } from "https://deno.land/std@0.201.0/log/mod.ts";
import {
  abortable,
  retry,
  RetryOptions,
} from "https://deno.land/std@0.201.0/async/mod.ts";
import { ulid } from "https://deno.land/x/ulid@v0.3.0/mod.ts";

const logger = () => getLogger("indexed_kv");

const neverSignal = new AbortController().signal;

const MAIN_INDEX_KEY = "id" as const;

/**
 * Options for initializing a {@link Store}.
 */
export interface StoreOptions<Item, IndexMap extends AnyIndexMap> {
  /**
   * Map of index options.
   */
  readonly indices?: IndexOptionsMap<Item, IndexMap>;
}

export type IndexOptionsMap<Item, IndexMap extends AnyIndexMap> = {
  readonly [IndexKey in keyof IndexMap]: IndexOptions<Item, IndexMap[IndexKey]>;
};

/**
 * Options for initializing a single index in a {@link Store}.
 */
export interface IndexOptions<Item, IndexValue> {
  /**
   * Computes the index value for an item.
   */
  readonly getValue: (item: Item) => IndexValue;
}

export type AnyIndexMap = Record<string, Deno.KvKeyPart>;

/**
 * Options for querying store items by an index.
 */
export interface IndexListSelector<IndexValue> {
  /**
   * Starting index value. Inclusive.
   */
  start?: IndexValue;

  /**
   * Ending index value. Exclusive.
   */
  end?: IndexValue;

  /**
   * Single index value to be queried.
   */
  value?: IndexValue;

  /**
   * Starting ID or date. Inclusive.
   *
   * Limits the results to items created after and including this date.
   *
   * Only works as expected when {@link IndexListSelector.value} is also passed.
   * Ignored otherwise.
   */
  after?: Date | string;

  /**
   * Ending ID or date. Exclusive.
   *
   * Limits the results to items created before this date.
   *
   * Only works as expected when {@link IndexListSelector.value} is also passed.
   * Ignored otherwise.
   */
  before?: Date | string;
}

/**
 * Options for querying all items in the store.
 */
export interface ListSelector {
  /**
   * Starting ID or date. Inclusive.
   *
   * Limits the results to items created after this date.
   */
  after?: Date | string;
  /**
   * Ending ID or date. Exclusive.
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
 * @template Item Type of items stored in the store.
 * @template IndexMap Map of index names to their types.
 */
export class Store<Item, IndexMap extends AnyIndexMap = {}> {
  /**
   * Kv database to use.
   */
  readonly db: Deno.Kv;

  /**
   * Key in Kv database under which this store is storing its data.
   */
  readonly key: Deno.KvKeyPart;

  /**
   * Store options.
   */
  readonly options: StoreOptions<Item, IndexMap>;

  /**
   * Defines a new store and provides methods for querying and modifying it's contents.
   *
   * Doesn't actually create anything in the database until the first item is created.
   */
  constructor(
    db: Deno.Kv,
    key: Deno.KvKeyPart,
    options: StoreOptions<Item, IndexMap> = {},
  ) {
    this.db = db;
    this.key = key;
    this.options = options;
  }

  /**
   * Creates a new item in the store.
   *
   * Adds item to the main index and all the defined indices.
   * Automatically assigns a new id to the item using {@link ulid}.
   */
  async create(value: Item): Promise<Model<Item>> {
    const id = ulid();
    await this.db.set([this.key, MAIN_INDEX_KEY, id], value);
    logger().debug(
      `Creating ${id}: Created ${[this.key, MAIN_INDEX_KEY, id].join("/")}`,
    );
    for (
      const [indexKey, index] of Object.entries(this.options.indices ?? {})
    ) {
      const indexValue = index.getValue(value);
      await this.db.set([this.key, indexKey, indexValue, id], value);
      logger().debug(
        `Creating ${id}: Created ${
          [this.key, indexKey, indexValue, id].join("/")
        }`,
      );
    }
    return new Model(this, id, value);
  }

  /**
   * Returns a single item from the store by it's ID.
   */
  async getById(id: string): Promise<Model<Item> | null> {
    const entry = await this.db.get<Item>([this.key, MAIN_INDEX_KEY, id]);
    if (entry.versionstamp == null) return null;
    return new Model(this, id, entry.value);
  }

  /**
   * Returns iterator over values based on an index.
   */
  async *listBy<IndexKey extends keyof IndexMap & string>(
    index: IndexKey,
    selector: IndexListSelector<IndexMap[IndexKey]>,
    options?: Deno.KvListOptions,
  ): AsyncIterableIterator<Model<Item>> {
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
      if (start && end) {
        kvSelector = { start, end };
      } //
      // if only one bound was passed, include prefix for whole index value
      else {
        kvSelector = {
          prefix: [this.key, index, selector.value],
          start,
          end,
        };
      }
    } //
    // if index value range was passed
    else {
      // request items limited by index value

      const start = selector.start != null
        ? [this.key, index, selector.start]
        : undefined;
      const end = selector.end != null
        ? [this.key, index, selector.end]
        : undefined;

      // if both bounds were passed, request specific range
      if (start && end) kvSelector = { start, end };
      // if only one bound was passed, include prefix for whole index
      else kvSelector = { prefix: [this.key, index], start, end };
    }

    for await (const entry of this.db.list<Item>(kvSelector, options)) {
      const [_storeKey, _index, _indexValue, id] = entry.key;
      if (typeof id !== "string") continue;
      yield new Model(this, id, entry.value);
    }
  }

  /**
   * Returns array of values based on an index.
   */
  async getBy<IndexKey extends keyof IndexMap & string>(
    index: IndexKey,
    selector: IndexListSelector<IndexMap[IndexKey]>,
    options?: Deno.KvListOptions & { signal?: AbortSignal },
  ): Promise<Array<Model<Item>>> {
    const items: Model<Item>[] = [];
    const signal = options?.signal ?? neverSignal;
    try {
      for await (
        const item of abortable(
          this.listBy(index, selector, options),
          signal,
        )
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
  ): AsyncIterableIterator<Model<Item>> {
    let kvSelector: Deno.KvListSelector;

    let start: Deno.KvKey | undefined;
    if (selector?.after != null) {
      const after = selector.after instanceof Date
        ? ulid(selector.after.getTime())
        : selector.after;
      start = [this.key, MAIN_INDEX_KEY, after];
    }

    let end: Deno.KvKey | undefined;
    if (selector?.before != null) {
      const before = selector.before instanceof Date
        ? ulid(selector.before.getTime())
        : selector.before;
      end = [this.key, MAIN_INDEX_KEY, before];
    }

    // if both bounds were passed, request specific range
    if (start && end) kvSelector = { start, end };
    // if only one bound was passed, include prefix for whole collection
    else kvSelector = { prefix: [this.key, MAIN_INDEX_KEY], start, end };

    for await (const entry of this.db.list<Item>(kvSelector, options)) {
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
  ): Promise<Array<Model<Item>>> {
    const items: Model<Item>[] = [];
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
      if (index === MAIN_INDEX_KEY) continue;
      await this.db.delete(entry.key);
      logger().info(`Rebuiliding indices: Deleted ${entry.key.join("/")}`);
    }

    // iterate over the new defined indices
    for (
      const [indexKey, index] of Object.entries(this.options.indices ?? {})
    ) {
      // iterate over the main index
      for await (
        const entry of this.db.list<Item>({
          prefix: [this.key, MAIN_INDEX_KEY],
        })
      ) {
        const [_storeKey, _indexKey, id] = entry.key;
        if (typeof id !== "string") continue;
        // get the index value
        const indexValue = index.getValue(entry.value);
        // add the item to the index
        await this.db.set([this.key, indexKey, indexValue, id], entry.value);
        logger().info(
          `Rebuiliding indices: Created ${
            [this.key, indexKey, indexValue, id].join("/")
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
  async migrate<OldItem>(
    updater: (value: OldItem) => Item,
  ): Promise<void> {
    const newValues: Record<string, Item> = {};
    // iterate over the main index
    for await (
      const entry of this.db.list<OldItem>({
        prefix: [this.key, MAIN_INDEX_KEY],
      })
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
        await retry(() => this.db.set([this.key, MAIN_INDEX_KEY, id], value));
        count++;
        logger().info(
          `Migrating: Committed ${[this.key, MAIN_INDEX_KEY, id].join("/")}`,
        );
      } catch (err) {
        throw new Error(
          `Migration failed (${count}/${totalCount} committed): Failed to set ${
            [this.key, MAIN_INDEX_KEY, id].join("/")
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
export class Model<Item> {
  /**
   * The {@link Store} this item belongs to.
   */
  readonly store: Store<Item, AnyIndexMap>;

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
  value: Item;

  /**
   * Creates a new model instance.
   * You shouldn't need to call this directly.
   * @internal
   */
  constructor(
    store: Store<Item, AnyIndexMap>,
    id: string,
    value: Item,
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
    updater?: Partial<Item> | ((value: Item) => Item),
  ): Promise<Item | null> {
    // get current main entry
    const oldEntry = await this.store.db.get<Item>([
      this.store.key,
      MAIN_INDEX_KEY,
      this.id,
    ]);

    // check if deleted
    if (oldEntry.versionstamp == null) {
      throw new Error(
        `Failed to update ${
          [this.store.key, MAIN_INDEX_KEY, this.id].join("/")
        }: Not found`,
      );
    }

    // get all current index entries
    const oldIndexEntries: Record<string, Deno.KvEntryMaybe<Item>> = {};
    for (
      const [indexKey, index] of Object.entries(
        this.store.options.indices ?? {},
      )
    ) {
      const indexValue = index.getValue(oldEntry.value);
      oldIndexEntries[indexKey] = await this.store.db.get<Item>([
        this.store.key,
        indexKey,
        indexValue,
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
      .set([this.store.key, MAIN_INDEX_KEY, this.id], this.value);
    logger().debug(
      `Updating ${this.id}: Updating ${
        [this.store.key, MAIN_INDEX_KEY, this.id].join("/")
      }`,
    );

    // update all index entries
    for (
      const [indexKey, index] of Object.entries(
        this.store.options.indices ?? {},
      )
    ) {
      const oldIndexValue = index.getValue(oldEntry.value);
      const newIndexValue: Deno.KvKeyPart = index.getValue(this.value);
      if (newIndexValue === oldIndexValue) {
        transaction
          .check(oldIndexEntries[indexKey])
          .set([this.store.key, indexKey, newIndexValue, this.id], this.value);
        logger().debug(
          `Updating ${this.id}: Updating ${
            [this.store.key, indexKey, newIndexValue, this.id].join("/")
          }`,
        );
      } else {
        transaction
          .check(oldIndexEntries[indexKey])
          .delete([this.store.key, indexKey, oldIndexValue, this.id])
          .set([this.store.key, indexKey, newIndexValue, this.id], this.value);
        logger().debug(
          `Updating ${this.id}: Deleting ${
            [this.store.key, indexKey, oldIndexValue, this.id].join("/")
          }`,
        );
        logger().debug(
          `Updating ${this.id}: Creating ${
            [this.store.key, indexKey, newIndexValue, this.id].join("/")
          }`,
        );
      }
    }

    // commit
    const result = await transaction.commit();
    logger().debug(`Updating ${this.id}: Committed`);
    if (!result.ok) {
      throw new Error(
        `Failed to update ${
          [this.store.key, MAIN_INDEX_KEY, this.id].join("/")
        }: Conflict`,
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
    updater?: Partial<Item> | ((value: Item) => Item),
    options?: RetryOptions,
  ): Promise<Item | null> {
    return retry(() => this.attemptUpdate(updater), options);
  }

  /**
   * Deletes the item from the store's main index and all the defined indices using an atomic transaction.
   *
   * The current {@link Model.value} is not removed and can be still accessed for example to create a new entry.
   */
  async attemptDelete(): Promise<void> {
    // get current main entry
    const entry = await this.store.db.get<Item>([
      this.store.key,
      MAIN_INDEX_KEY,
      this.id,
    ]);

    // check if already deleted
    if (entry.versionstamp == null) {
      throw new Error(
        `Failed to delete ${
          [this.store.key, MAIN_INDEX_KEY, this.id].join("/")
        }: Not found`,
      );
    }

    // begin transaction
    const transaction = this.store.db.atomic();

    // delete main entry
    transaction
      .check(entry)
      .delete([this.store.key, MAIN_INDEX_KEY, this.id]);
    logger().debug(
      `Deleting ${this.id}: Deleting ${
        [this.store.key, MAIN_INDEX_KEY, this.id].join("/")
      }`,
    );

    // delete all index entries
    for (
      const [indexKey, index] of Object.entries(
        this.store.options.indices ?? {},
      )
    ) {
      const indexValue = index.getValue(entry.value);
      transaction
        .delete([this.store.key, indexKey, indexValue, this.id]);
      logger().debug(
        `Deleting ${this.id}: Deleting ${
          [this.store.key, indexKey, indexValue, this.id].join("/")
        }`,
      );
    }

    // commit
    const result = await transaction.commit();
    logger().debug(`Deleting ${this.id}: Committed`);
    if (!result.ok) {
      throw new Error(
        `Failed to delete ${
          [this.store.key, MAIN_INDEX_KEY, this.id].join("/")
        }: Conflict`,
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
