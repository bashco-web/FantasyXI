import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mapDepositErrorToMessage } from "./depositErrorMessage.js";
import { SorobanDepositTimeoutError } from "./sorobanDeposit.js";

describe("mapDepositErrorToMessage", () => {
  it("maps a Freighter rejection to a friendly message", () => {
    assert.equal(
      mapDepositErrorToMessage(new Error("User declined access")),
      "Signature request was rejected in the selected wallet."
    );
    assert.equal(
      mapDepositErrorToMessage(new Error("Transaction rejected by user")),
      "Signature request was rejected in the selected wallet."
    );
  });

  it("maps insufficient balance / trustline errors", () => {
    assert.equal(
      mapDepositErrorToMessage(new Error("Contract simulation failed: missing trustline")),
      "Insufficient Testnet USDC balance or missing trustline in Freighter."
    );
    assert.equal(
      mapDepositErrorToMessage(new Error("balance too low")),
      "Insufficient Testnet USDC balance or missing trustline in Freighter."
    );
  });

  it("maps network/connectivity failures", () => {
    assert.equal(
      mapDepositErrorToMessage(new Error("Failed to load account GABC123")),
      "Could not reach the Stellar network. Check your connection and retry."
    );
    assert.equal(
      mapDepositErrorToMessage(new Error("NetworkError when attempting to fetch")),
      "Could not reach the Stellar network. Check your connection and retry."
    );
  });

  it("falls back to the raw message for unrecognized errors", () => {
    assert.equal(
      mapDepositErrorToMessage(new Error("Some unexpected contract error")),
      "Some unexpected contract error"
    );
  });

  it("stringifies non-Error values", () => {
    assert.equal(mapDepositErrorToMessage("plain string failure"), "plain string failure");
  });
});

describe("SorobanDepositTimeoutError", () => {
  it("carries the transaction hash so a retry can re-check status without resubmitting", () => {
    const err = new SorobanDepositTimeoutError("abc123txhash");
    assert.ok(err instanceof Error);
    assert.equal(err.name, "SorobanDepositTimeoutError");
    assert.equal(err.txHash, "abc123txhash");
    assert.match(err.message, /could not confirm its status/);
  });
});
