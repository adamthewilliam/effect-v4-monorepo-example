import { Schema } from "effect";
import { HttpApiSchema } from "effect/unstable/httpapi";
import type { HttpApiSchemaError } from "effect/unstable/httpapi/HttpApiError";
import type { DatabaseUnavailableError } from "../services/DexTradeErrors";

const messageErrorPayload = <Tag extends string>(tag: Tag) =>
  Schema.TaggedStruct(tag, { message: Schema.String });

export const ServiceUnavailableErrorPayload = messageErrorPayload("ServiceUnavailable").annotate({
  identifier: "ServiceUnavailableError",
  description: "The service is temporarily unable to handle the request.",
});

export const InternalServerErrorPayload = messageErrorPayload("InternalServerError").annotate({
  identifier: "InternalServerError",
  description: "An unexpected server failure occurred.",
});

export const InvalidRequestErrorPayload = Schema.TaggedStruct("InvalidRequest", {
  message: Schema.String,
  location: Schema.optionalKey(Schema.String),
}).annotate({
  identifier: "InvalidRequestError",
  description: "The request failed schema validation.",
});

export const InternalServerErrorResponse = Schema.Struct({
  error: InternalServerErrorPayload,
})
  .pipe(HttpApiSchema.status(500))
  .annotate({
    identifier: "InternalServerErrorResponse",
    description: "Unexpected server failure response.",
  });

export const InvalidRequestErrorResponse = Schema.Struct({
  error: InvalidRequestErrorPayload,
})
  .pipe(HttpApiSchema.status(400))
  .annotate({
    identifier: "InvalidRequestResponse",
    description: "The request did not match the endpoint schema.",
  });

export const ServiceUnavailableErrorResponse = Schema.Struct({
  error: ServiceUnavailableErrorPayload,
})
  .pipe(HttpApiSchema.status(503))
  .annotate({
    identifier: "ServiceUnavailableResponse",
    description: "A required dependency is temporarily unavailable.",
  });

export const ReadinessUnavailableErrorResponse = Schema.Struct({
  status: Schema.Literal("not_ready"),
  error: ServiceUnavailableErrorPayload,
})
  .pipe(HttpApiSchema.status(503))
  .annotate({
    identifier: "ReadinessUnavailableResponse",
    description: "The service process is healthy, but a required dependency is unavailable.",
  });

export const ReadinessErrorResponses = [ReadinessUnavailableErrorResponse] as const;

export const ReadinessErrorResponse = Schema.Union([ReadinessUnavailableErrorResponse]);
export type ReadinessErrorResponse = typeof ReadinessErrorResponse.Type;
export type InternalServerErrorResponseBody = typeof InternalServerErrorResponse.Type;
export type InvalidRequestErrorResponseBody = typeof InvalidRequestErrorResponse.Type;
export type ServiceUnavailableErrorResponseBody = typeof ServiceUnavailableErrorResponse.Type;

export function toReadinessErrorResponse(_error: DatabaseUnavailableError): ReadinessErrorResponse {
  return {
    status: "not_ready",
    error: {
      _tag: "ServiceUnavailable",
      message: "Service temporarily unavailable",
    },
  };
}

export function toInternalServerErrorResponse(): InternalServerErrorResponseBody {
  return {
    error: {
      _tag: "InternalServerError",
      message: "Internal server error",
    },
  };
}

export function toInvalidRequestErrorResponse(
  error: HttpApiSchemaError,
): InvalidRequestErrorResponseBody {
  return {
    error: {
      _tag: "InvalidRequest",
      message: "Invalid request",
      location: error.kind,
    },
  };
}

export function toServiceUnavailableErrorResponse(): ServiceUnavailableErrorResponseBody {
  return {
    error: {
      _tag: "ServiceUnavailable",
      message: "Service temporarily unavailable",
    },
  };
}
