# Indexed KV

[![deno doc](https://doc.deno.land/badge.svg)](https://deno.land/x/indexed_kv/mod.ts)

Small wrapper for Deno KV.

## Example

```ts
import { Schema, Store } from "https://deno.land/x/indexed_kv/mod.ts";

const db = await Deno.openKv();

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
  indices: ["requestedBy", "status.type"], // TypeScript autocompletes these!
});

const job = await jobStore.create({
  requestedBy: "test",
  params: { a: 1, b: null },
  status: { type: "idle" },
  lastUpdateDate: new Date(),
});

const jobsByUser = await jobStore.getBy("requestedBy", "test");

await jobsByUser[0].update({ status: { type: "processing", progress: 0 } });

const jobsByStatus = await jobStore.getBy("status.type", "processing");

await jobsByStatus[0].delete();
```
