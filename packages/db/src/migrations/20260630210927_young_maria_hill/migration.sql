CREATE TABLE "dex_trades" (
	"unique_id" uuid PRIMARY KEY NOT NULL,
	"block_timestamp" timestamp with time zone NOT NULL,
	"signer" text NOT NULL,
	"token_sold_amount" numeric(38, 18) NOT NULL,
	"usd_sold_amount" numeric(38, 8) NOT NULL,
	"token_bought_amount" numeric(38, 18) NOT NULL,
	"usd_bought_amount" numeric(38, 8) NOT NULL,
	"aggregator" text NOT NULL,
	"tx_fee_usd" numeric(38, 8) NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX "dex_trades_block_timestamp_idx" ON "dex_trades" USING btree ("block_timestamp");--> statement-breakpoint
CREATE INDEX "dex_trades_aggregator_idx" ON "dex_trades" USING btree ("aggregator");--> statement-breakpoint
CREATE INDEX "dex_trades_aggregator_block_timestamp_idx" ON "dex_trades" USING btree ("aggregator","block_timestamp");--> statement-breakpoint
CREATE INDEX "dex_trades_signer_idx" ON "dex_trades" USING btree ("signer");
