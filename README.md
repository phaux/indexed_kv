# Indexed KV

[![deno doc](https://doc.deno.land/badge.svg)](https://deno.land/x/indexed_kv/mod.ts)

Small wrapper for Deno KV.

## Example

```ts
import { Store } from "https://deno.land/x/indexed_kv/mod.ts";

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

type JobIndices = {
  requestedBy: string;
  statusType: JobSchema["status"]["type"];
};

const jobStore = new Store<JobSchema, JobIndices>(db, "jobs", {
  indices: {
    requestedBy: {
      getValue: (job) => job.requestedBy,
    },
    statusType: {
      getValue: (job) => job.status.type,
    },
  },
});

const job = await jobStore.create({
  requestedBy: "test",
  params: { a: 1, b: null },
  status: { type: "idle" },
  lastUpdateDate: new Date(),
});

// get all requested by user name beginning with "t"
const jobsByUser = await jobStore.getBy("requestedBy", {
  start: "t",
  end: "u",
});

await jobsByUser[0].update({
  status: { type: "processing", progress: 0 },
});

// get all where status is processing
const jobsByStatus = await jobStore.getBy("statusType", {
  value: "processing",
});

await jobsByStatus[0].delete();
```
