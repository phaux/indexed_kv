import { assert } from "https://deno.land/std@0.198.0/assert/assert.ts";
import { Schema, Store } from "./mod.ts";
import { handlers, setup } from "https://deno.land/std@0.201.0/log/mod.ts";

setup({
  handlers: {
    console: new handlers.ConsoleHandler("DEBUG"),
  },
  loggers: {
    indexed_kv: { level: "DEBUG", handlers: ["console"] },
  },
});

const db = await Deno.openKv();

interface PointSchema {
  x: number;
  y: number;
}

const pointStore = new Store(db, "points", {
  schema: new Schema<PointSchema>(),
  indices: ["x", "y"],
});

interface JobSchema {
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

const jobStore = new Store(db, "jobs", {
  schema: new Schema<JobSchema>(),
  indices: ["requestedBy", "status.type"],
});

Deno.test("create and delete", async () => {
  await pointStore.deleteAll();
  const point1 = await pointStore.create({ x: 1, y: 2 });
  const point2 = await pointStore.create({ x: 3, y: 4 });
  assert((await pointStore.getAll()).length === 2);
  const point3 = await pointStore.create({ x: 5, y: 6 });
  assert((await pointStore.getAll()).length === 3);
  assert((await pointStore.getById(point2.id))?.value.y === 4);
  await point1.delete();
  assert((await pointStore.getAll()).length === 2);
  await point2.delete();
  await point3.delete();
  assert((await pointStore.getAll()).length === 0);
});

Deno.test("list by index", async () => {
  await jobStore.deleteAll();

  const job = await jobStore.create({
    requestedBy: "test",
    params: { a: 1, b: null },
    status: { type: "idle" },
    lastUpdateDate: new Date(),
  });
  assert((await jobStore.getBy("requestedBy", "test"))[0].value.params.a === 1);
  assert((await jobStore.getBy("status.type", "idle"))[0].value.params.a === 1);

  await job.update({ status: { type: "processing", progress: 33 } });
  assert(
    (await jobStore.getBy("status.type", "processing"))[0].value.params.a === 1,
  );

  await job.update({ status: { type: "done" } });
  assert((await jobStore.getBy("status.type", "done"))[0].value.params.a === 1);
  assert((await jobStore.getBy("status.type", "processing")).length === 0);

  await job.delete();
  assert((await jobStore.getBy("status.type", "done")).length === 0);
  assert((await jobStore.getBy("requestedBy", "test")).length === 0);
});

Deno.test("fail on concurrent update", async () => {
  await jobStore.deleteAll();

  const job = await jobStore.create({
    requestedBy: "test",
    params: { a: 1, b: null },
    status: { type: "idle" },
    lastUpdateDate: new Date(),
  });

  const result = await Promise.all([
    job.attemptUpdate({ status: { type: "processing", progress: 33 } }),
    job.attemptUpdate({ status: { type: "done" } }),
  ]).catch(() => true);
  assert(result === true);

  await job.delete();
});

Deno.test("rebuilds indices", async () => {
  type OldJobSchema = {
    requestedBy: string;
    params: {
      a: number;
    };
  };

  const oldJobStore = new Store(db, "jobs", {
    schema: new Schema<OldJobSchema>(),
    indices: ["params.a"],
  });

  await oldJobStore.deleteAll();
  await oldJobStore.create({ requestedBy: "a", params: { a: 1 } });
  await oldJobStore.create({ requestedBy: "a", params: { a: 2 } });
  await oldJobStore.create({ requestedBy: "b", params: { a: 3 } });
  assert((await oldJobStore.getBy("params.a", 1))[0].value.requestedBy === "a");
  assert((await oldJobStore.getBy("params.a", 3))[0].value.requestedBy === "b");

  const newJobStore = new Store(db, "jobs", {
    schema: new Schema<OldJobSchema>(),
    indices: ["requestedBy"],
  });

  assert((await newJobStore.getBy("requestedBy", "a")).length === 0);

  await newJobStore.rebuildIndices();

  assert((await newJobStore.getBy("requestedBy", "a")).length === 2);
  assert((await oldJobStore.getBy("params.a", 1)).length === 0);

  await newJobStore.deleteAll();
});
