import { index, numeric, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { Schema } from "effect";

const NonBlankText = Schema.String.check(
  Schema.isNonEmpty(),
  Schema.isPattern(/\S/, { message: "Must contain a non-whitespace character" }),
  Schema.isTrimmed({ message: "Must not include leading or trailing whitespace" }),
);

const NonNegativeDecimalString = Schema.String.check(
  Schema.isPattern(/^\d+(?:\.\d+)?$/, { message: "Expected a non-negative decimal string" }),
);

const SignedDecimalString = Schema.String.check(
  Schema.isPattern(/^-?\d+(?:\.\d+)?$/, { message: "Expected a signed decimal string" }),
);

export const DexTradeId = Schema.String.check(Schema.isUUID(4)).pipe(
  Schema.brand("@effect-monorepo/DexTradeId"),
);
export type DexTradeId = typeof DexTradeId.Type;

export const SignerAddress = NonBlankText.pipe(Schema.brand("@effect-monorepo/SignerAddress"));
export type SignerAddress = typeof SignerAddress.Type;

export const AggregatorName = NonBlankText.pipe(Schema.brand("@effect-monorepo/AggregatorName"));
export type AggregatorName = typeof AggregatorName.Type;

// Decimals are represented as strings by Drizzle but persisted values are Postgres numeric
export const TokenAmountDecimal = NonNegativeDecimalString.pipe(
  Schema.brand("@effect-monorepo/TokenAmountDecimal"),
);
export type TokenAmountDecimal = typeof TokenAmountDecimal.Type;

export const UsdAmountDecimal = NonNegativeDecimalString.pipe(
  Schema.brand("@effect-monorepo/UsdAmountDecimal"),
);
export type UsdAmountDecimal = typeof UsdAmountDecimal.Type;

export const PnlUsdDecimal = SignedDecimalString.pipe(
  Schema.brand("@effect-monorepo/PnlUsdDecimal"),
);
export type PnlUsdDecimal = typeof PnlUsdDecimal.Type;

export const dexTrades = pgTable(
  "dex_trades",
  {
    uniqueId: uuid("unique_id").$type<DexTradeId>().primaryKey(),
    blockTimestamp: timestamp("block_timestamp", { withTimezone: true }).notNull(),
    signer: text("signer").$type<SignerAddress>().notNull(),
    tokenSoldAmount: numeric("token_sold_amount", { precision: 38, scale: 18 })
      .$type<TokenAmountDecimal>()
      .notNull(),
    usdSoldAmount: numeric("usd_sold_amount", { precision: 38, scale: 8 })
      .$type<UsdAmountDecimal>()
      .notNull(),
    tokenBoughtAmount: numeric("token_bought_amount", { precision: 38, scale: 18 })
      .$type<TokenAmountDecimal>()
      .notNull(),
    usdBoughtAmount: numeric("usd_bought_amount", { precision: 38, scale: 8 })
      .$type<UsdAmountDecimal>()
      .notNull(),
    aggregator: text("aggregator").$type<AggregatorName>().notNull(),
    txFeeUsd: numeric("tx_fee_usd", { precision: 38, scale: 8 })
      .$type<UsdAmountDecimal>()
      .notNull(),
    pnlUsd: numeric("pnl_usd", { precision: 38, scale: 8 })
      .$type<PnlUsdDecimal>()
      .default(sql`0`)
      .notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  },
  (table) => [
    index("dex_trades_block_timestamp_idx").on(table.blockTimestamp),
    index("dex_trades_aggregator_idx").on(table.aggregator),
    index("dex_trades_aggregator_block_timestamp_idx").on(table.aggregator, table.blockTimestamp),
    index("dex_trades_signer_idx").on(table.signer),
  ],
);

export type DexTrade = typeof dexTrades.$inferSelect;
export type NewDexTrade = typeof dexTrades.$inferInsert;
