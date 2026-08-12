import { BunRuntime } from "@effect/platform-bun";
import { Layer } from "effect";
import { HttpLive } from "./http/server";

BunRuntime.runMain(Layer.launch(HttpLive));
