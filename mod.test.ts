import { ConsoleHandler } from "https://deno.land/std@0.201.0/log/handlers.ts";
import { Store } from "./mod.ts";
import { setup } from "https://deno.land/std@0.201.0/log/mod.ts";
import { assertEquals } from "https://deno.land/std@0.195.0/assert/assert_equals.ts";
import { assertRejects } from "https://deno.land/std@0.195.0/assert/assert_rejects.ts";
import { assert } from "https://deno.land/std@0.201.0/assert/assert.ts";

setup({
  handlers: {
    console: new ConsoleHandler("DEBUG"),
  },
  loggers: {
    indexed_kv: { level: "DEBUG", handlers: ["console"] },
  },
});

const db = await Deno.openKv();

Deno.test("create and delete", async () => {
  interface PointSchema {
    x: number;
    y: number;
  }

  const pointStore = new Store<PointSchema>(db, "points", { indices: {} });

  await pointStore.deleteAll();
  const point1 = await pointStore.create({ x: 1, y: 2 });
  const point2 = await pointStore.create({ x: 3, y: 4 });
  assertEquals((await pointStore.getAll()).length, 2);
  const point3 = await pointStore.create({ x: 5, y: 6 });
  assertEquals((await pointStore.getAll()).length, 3);
  assertEquals((await pointStore.getById(point2.id))?.value, { x: 3, y: 4 });
  await point1.delete();
  assertEquals((await pointStore.getAll()).length, 2);
  await point2.delete();
  await point3.delete();
  assertEquals((await pointStore.getAll()).length, 0);
});

Deno.test("list by index", async () => {
  interface TaskSchema {
    requestedBy: string;
    params: {
      a: number;
      b: number | null;
    };
    status:
      | { type: "idle" }
      | { type: "processing"; progress: number }
      | { type: "done" };
    lastUpdateDate: Date;
  }

  type TaskIndices = {
    "requestedBy": string;
    "status.type": "idle" | "processing" | "done";
  };

  const taskStore = new Store<TaskSchema, TaskIndices>(db, "tasks", {
    indices: {
      "requestedBy": { getValue: (item) => item.requestedBy },
      "status.type": { getValue: (item) => item.status.type },
    },
  });

  await taskStore.deleteAll();

  const task = await taskStore.create({
    requestedBy: "test",
    params: { a: 1, b: null },
    status: { type: "idle" },
    lastUpdateDate: new Date(),
  });
  assertEquals(
    (await taskStore.getBy("requestedBy", { value: "test" }))[0].value
      .params,
    { a: 1, b: null },
  );
  assertEquals(
    (await taskStore.getBy("status.type", { value: "idle" }))[0].value
      .params,
    { a: 1, b: null },
  );

  await task.update({ status: { type: "processing", progress: 33 } });
  assertEquals(
    (await taskStore.getBy("status.type", { value: "processing" }))[0]
      .value
      .params,
    { a: 1, b: null },
  );

  await task.update({ status: { type: "done" } });
  assertEquals(
    (await taskStore.getBy("status.type", { value: "done" }))[0].value
      .params,
    { a: 1, b: null },
  );
  assertEquals(
    (await taskStore.getBy("status.type", { value: "processing" })).length,
    0,
  );

  await task.delete();
  assertEquals(
    (await taskStore.getBy("status.type", { value: "done" })).length,
    0,
  );
  assertEquals(
    (await taskStore.getBy("requestedBy", { value: "test" })).length,
    0,
  );

  await taskStore.create({
    requestedBy: "test1",
    params: { a: 1, b: null },
    status: { type: "idle" },
    lastUpdateDate: new Date(),
  });
  await taskStore.create({
    requestedBy: "test2",
    params: { a: 1, b: null },
    status: { type: "idle" },
    lastUpdateDate: new Date(),
  });
  await taskStore.create({
    requestedBy: "test3",
    params: { a: 1, b: null },
    status: { type: "idle" },
    lastUpdateDate: new Date(),
  });

  assertEquals(
    (await taskStore.getBy("requestedBy", {})).length,
    3,
  );
  assertEquals(
    (await taskStore.getBy("requestedBy", { start: "test2" })).length,
    2,
  );
  assertEquals(
    (await taskStore.getBy("requestedBy", { end: "test3" })).length,
    2,
  );

  await taskStore.deleteAll();
});

Deno.test("fail on concurrent update", async () => {
  const fooStore = new Store<
    { a: string; b: string; c: string },
    { "a": string; "b": string; "c": string }
  >(db, "foos", {
    indices: {
      "a": { getValue: (item) => item.a },
      "b": { getValue: (item) => item.b },
      "c": { getValue: (item) => item.c },
    },
  });

  await fooStore.deleteAll();

  const foo = await fooStore.create({
    a: "a",
    b: "b",
    c: "c",
  });

  await assertRejects(async () => {
    await Promise.all([
      foo.attemptUpdate({ a: "aa" }),
      foo.attemptUpdate({ a: "aaa" }),
    ]);
  });

  await foo.delete();
});

Deno.test("rebuilds indices", async () => {
  type OldTaskSchema = {
    requestedBy: string;
    params: {
      a: number;
    };
  };

  const oldTaskStore = new Store<OldTaskSchema, { a: number }>(db, "tasks", {
    indices: {
      "a": { getValue: (task) => task.params.a },
    },
  });

  await oldTaskStore.deleteAll();
  await oldTaskStore.create({ requestedBy: "user_a", params: { a: 1 } });
  await oldTaskStore.create({ requestedBy: "user_a", params: { a: 2 } });
  await oldTaskStore.create({ requestedBy: "user_b", params: { a: 3 } });
  assertEquals(
    (await oldTaskStore.getBy("a", { value: 1 }))[0].value,
    { requestedBy: "user_a", params: { a: 1 } },
  );
  assertEquals(
    (await oldTaskStore.getBy("a", { value: 3 }))[0].value,
    { requestedBy: "user_b", params: { a: 3 } },
  );

  const newTaskStore = new Store<OldTaskSchema, { requestedBy: string }>(
    db,
    "tasks",
    {
      indices: {
        "requestedBy": { getValue: (task) => task.requestedBy },
      },
    },
  );

  assertEquals(
    (await newTaskStore.getBy("requestedBy", { value: "user_a" })).length,
    0,
  );

  await newTaskStore.rebuildIndices();

  assertEquals(
    (await newTaskStore.getBy("requestedBy", { value: "user_a" })).length,
    2,
  );
  assertEquals((await oldTaskStore.getBy("a", { value: 1 })).length, 0);

  await newTaskStore.deleteAll();
});

Deno.test("migrate", async () => {
  type FooRequest = { a: number };
  type BarRequest = { b: number };
  type User = { id: number };
  type Chat = { id: number };
  type Message = { message_id: number; from: User; chat: Chat };

  interface JobSchemaV1 {
    params: Partial<FooRequest>;
    request: Message;
    reply?: Message;
    status:
      | { type: "waiting" }
      | { type: "processing"; progress: number }
      | { type: "done" };
  }

  interface JobSchema {
    task:
      | { type: "foo"; params: Partial<FooRequest> }
      | { type: "bar"; params: Partial<BarRequest> };
    user: User;
    chat: Chat;
    requestMessageId: number;
    replyMessageId?: number;
    status:
      | { type: "waiting" }
      | { type: "processing"; progress: number }
      | { type: "done" };
  }

  type JobIndicesV1 = {
    "status.type": JobSchemaV1["status"]["type"];
  };

  const jobStoreV1 = new Store<JobSchemaV1, JobIndicesV1>(db, "job1", {
    indices: {
      "status.type": { getValue: (job) => job.status.type },
    },
  });

  type JobIndices = {
    "status.type": JobSchema["status"]["type"];
    "user.id": User["id"];
  };

  const jobStore = new Store<JobSchema, JobIndices>(db, "job2", {
    indices: {
      "status.type": { getValue: (job) => job.status.type },
      "user.id": { getValue: (job) => job.user.id },
    },
  });

  await jobStoreV1.deleteAll();

  const job1 = await jobStoreV1.create({
    params: { a: 1 },
    request: { message_id: 1, from: { id: 1 }, chat: { id: 1 } },
    status: { type: "waiting" },
  });
  const job2 = await jobStoreV1.create({
    params: { a: 2 },
    request: { message_id: 2, from: { id: 2 }, chat: { id: 2 } },
    reply: undefined,
    status: { type: "processing", progress: 10 },
  });
  const job3 = await jobStoreV1.create({
    params: { a: 3 },
    request: { message_id: 3, from: { id: 3 }, chat: { id: 3 } },
    reply: { message_id: 4, from: { id: 4 }, chat: { id: 4 } },
    status: { type: "done" },
  });

  await jobStore.migrate((job: JobSchemaV1) => ({
    task: { type: "foo", params: job.params },
    user: job.request.from,
    chat: job.request.chat,
    requestMessageId: job.request.message_id,
    replyMessageId: job.reply?.message_id,
    status: job.status,
  }), { oldKey: "job1" });

  const newJob1 = await jobStore.getById(job1.id);
  const newJob2 = await jobStore.getById(job2.id);
  const newJob3 = await jobStore.getById(job3.id);

  assertEquals(newJob1?.value, {
    task: { type: "foo", params: { a: 1 } },
    user: { id: 1 },
    chat: { id: 1 },
    requestMessageId: 1,
    replyMessageId: undefined,
    status: { type: "waiting" },
  });
  assertEquals(newJob2?.value, {
    task: { type: "foo", params: { a: 2 } },
    user: { id: 2 },
    chat: { id: 2 },
    requestMessageId: 2,
    replyMessageId: undefined,
    status: { type: "processing", progress: 10 },
  });
  assertEquals(newJob3?.value, {
    task: { type: "foo", params: { a: 3 } },
    user: { id: 3 },
    chat: { id: 3 },
    requestMessageId: 3,
    replyMessageId: 4,
    status: { type: "done" },
  });

  assertEquals(
    (await jobStore.getBy("user.id", { value: 2 }))[0]?.value,
    undefined,
  );
  await jobStore.rebuildIndices();
  assertEquals((await jobStore.getBy("user.id", { value: 2 }))[0].value, {
    task: { type: "foo", params: { a: 2 } },
    user: { id: 2 },
    chat: { id: 2 },
    requestMessageId: 2,
    replyMessageId: undefined,
    status: { type: "processing", progress: 10 },
  });

  await jobStore.deleteAll();
});

Deno.test("index copy vs reference", async () => {
  const fooStore = new Store<{ x: number }, { x: number }>(db, "foos", {
    indices: {
      "x": { copy: true, getValue: (item) => item.x },
    },
  });
  await fooStore.deleteAll();

  const foo = await fooStore.create({ x: 123 });

  {
    const entry = await db.get(["foos", "id", foo.id]);
    assertEquals(entry.value, { x: 123 });
  }
  {
    const entry = await db.get(["foos", "x", 123, foo.id]);
    assertEquals(entry.value, { x: 123 });
  }

  await foo.update({ x: 456 });

  {
    const entry = await db.get(["foos", "id", foo.id]);
    assertEquals(entry.value, { x: 456 });
  }
  {
    const entry = await db.get(["foos", "x", 123, foo.id]);
    assert(entry.versionstamp == null);
  }
  {
    const entry = await db.get(["foos", "x", 456, foo.id]);
    assertEquals(entry.value, { x: 456 });
  }

  await fooStore.deleteAll();

  const barStore = new Store<{ x: number }, { x: number }>(db, "bars", {
    indices: {
      "x": { copy: false, getValue: (item) => item.x },
    },
  });
  await barStore.deleteAll();

  const bar = await barStore.create({ x: 123 });

  {
    const entry = await db.get(["bars", "id", bar.id]);
    assertEquals(entry.value, { x: 123 });
  }
  {
    const entry = await db.get(["bars", "x", 123, bar.id]);
    assert(entry.versionstamp != null);
    assertEquals(entry.value, null);
  }

  await bar.update({ x: 456 });

  {
    const entry = await db.get(["bars", "id", bar.id]);
    assertEquals(entry.value, { x: 456 });
  }
  {
    const entry = await db.get(["bars", "x", 123, bar.id]);
    assert(entry.versionstamp == null);
  }
  {
    const entry = await db.get(["bars", "x", 456, bar.id]);
    assert(entry.versionstamp != null);
    assertEquals(entry.value, null);
  }

  await barStore.deleteAll();
});

Deno.test("index value null", async () => {
  const fooStore = new Store<{ x: number | null }, { x: number | null }>(
    db,
    "foos",
    {
      indices: {
        "x": { getValue: (item) => item.x },
      },
    },
  );
  await fooStore.deleteAll();
  await fooStore.create({ x: null });
  await fooStore.create({ x: 123 });
  await fooStore.create({ x: null });
  await fooStore.create({ x: 456 });
  {
    const foos = await fooStore.getBy("x", {});
    assertEquals(foos.length, 2);
  }
  {
    // @ts-expect-error - can't query by null
    const foos = await fooStore.getBy("x", { value: null });
    assertEquals(foos.length, 2); // no value specified, so all items are returned
  }
  await fooStore.deleteAll();
});
