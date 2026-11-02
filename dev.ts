import frontend from "./web/index.html";
import { startServer } from "./server";

if (process.env.NODE_ENV === "production")
  throw new Error("The development launcher cannot run in production");
await startServer(frontend);
