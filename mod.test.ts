import { Schema, Store } from "./mod.ts";
import { handlers, setup } from "https://deno.land/std@0.201.0/log/mod.ts";
import {
  assert,
  assertEquals,
  assertRejects,
} from "https://deno.land/std@0.135.0/testing/asserts.ts";

setup({
  handlers: {
    console: new handlers.ConsoleHandler("DEBUG"),
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

  const pointStore = new Store(db, "points", {
    schema: new Schema<PointSchema>(),
    indices: ["x", "y"],
  });

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

  const taskStore = new Store(db, "tasks", {
    schema: new Schema<TaskSchema>(),
    indices: ["requestedBy", "status.type"],
  });

  await taskStore.deleteAll();

  const task = await taskStore.create({
    requestedBy: "test",
    params: { a: 1, b: null },
    status: { type: "idle" },
    lastUpdateDate: new Date(),
  });
  assertEquals(
    (await taskStore.getBy("requestedBy", { value: "test" }))[0].value.params,
    { a: 1, b: null },
  );
  assertEquals(
    (await taskStore.getBy("status.type", { value: "idle" }))[0].value.params,
    { a: 1, b: null },
  );

  await task.update({ status: { type: "processing", progress: 33 } });
  assertEquals(
    (await taskStore.getBy("status.type", { value: "processing" }))[0].value
      .params,
    { a: 1, b: null },
  );

  await task.update({ status: { type: "done" } });
  assertEquals(
    (await taskStore.getBy("status.type", { value: "done" }))[0].value.params,
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
  const fooStore = new Store(db, "foos", {
    schema: new Schema<{ a: string; b: string; c: string }>(),
    indices: ["a", "b", "c"],
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

  const oldTaskStore = new Store(db, "tasks", {
    schema: new Schema<OldTaskSchema>(),
    indices: ["params.a"],
  });

  await oldTaskStore.deleteAll();
  await oldTaskStore.create({ requestedBy: "a", params: { a: 1 } });
  await oldTaskStore.create({ requestedBy: "a", params: { a: 2 } });
  await oldTaskStore.create({ requestedBy: "b", params: { a: 3 } });
  assertEquals(
    (await oldTaskStore.getBy("params.a", { value: 1 }))[0].value,
    { requestedBy: "a", params: { a: 1 } },
  );
  assertEquals(
    (await oldTaskStore.getBy("params.a", { value: 3 }))[0].value,
    { requestedBy: "b", params: { a: 3 } },
  );

  const newTaskStore = new Store(db, "tasks", {
    schema: new Schema<OldTaskSchema>(),
    indices: ["requestedBy"],
  });

  assertEquals(
    (await newTaskStore.getBy("requestedBy", { value: "a" })).length,
    0,
  );

  await newTaskStore.rebuildIndices();

  assertEquals(
    (await newTaskStore.getBy("requestedBy", { value: "a" })).length,
    2,
  );
  assertEquals((await oldTaskStore.getBy("params.a", { value: 1 })).length, 0);

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

  const jobStoreV1 = new Store(db, "job", {
    schema: new Schema<JobSchemaV1>(),
    indices: ["status.type"],
  });

  const jobStore = new Store(db, "job", {
    schema: new Schema<JobSchema>(),
    indices: ["status.type", "user.id"],
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
  }));

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
