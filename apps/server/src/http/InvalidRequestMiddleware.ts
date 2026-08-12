import { Effect } from "effect";
import { HttpApiMiddleware } from "effect/unstable/httpapi";
import {
  InvalidRequestErrorResponse,
  toInvalidRequestErrorResponse,
} from "../contracts/ServerErrors";

export class InvalidRequestMiddleware extends HttpApiMiddleware.Service<InvalidRequestMiddleware>()(
  "@effect-monorepo/server/InvalidRequestMiddleware",
  {
    error: InvalidRequestErrorResponse,
  },
) {}

export const InvalidRequestMiddlewareLive = HttpApiMiddleware.layerSchemaErrorTransform(
  InvalidRequestMiddleware,
  (error) => {
    if (error.kind === "Body") {
      return Effect.logError("API response schema encoding failed", {
        cause: error.cause,
      }).pipe(Effect.flatMap(() => Effect.die(error)));
    }

    return Effect.logWarning("Invalid API request", {
      kind: error.kind,
      message: error.message,
    }).pipe(Effect.flatMap(() => Effect.fail(toInvalidRequestErrorResponse(error))));
  },
);
