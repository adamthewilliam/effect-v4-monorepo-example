import { Effect } from "effect";
import { HttpMiddleware, HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { HttpApiSchemaError } from "effect/unstable/httpapi/HttpApiError";
import { toInternalServerErrorResponse } from "../contracts/ServerErrors";

export const GlobalUnexpectedErrorMiddlewareLive = HttpRouter.middleware(
  HttpMiddleware.make((app) =>
    app.pipe(
      Effect.catchDefect((defect) => {
        if (HttpApiSchemaError.is(defect) && defect.kind !== "Body") {
          return Effect.die(defect);
        }

        return Effect.logError("Unexpected API defect", { defect }).pipe(
          Effect.flatMap(() =>
            HttpServerResponse.json(toInternalServerErrorResponse(), { status: 500 }),
          ),
        );
      }),
    ),
  ),
  { global: true },
);
