import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "block_num_eq_join",
  network: "anvil",
  dependencies: {
    anvil_rpc: "_/anvil_rpc@0.0.0",
  },
  tables: {
    // Two tables with distinct per-block values, designed to be joined on block_num.
    // left_val and right_val are non-join columns with predictable, different values
    // per block, so result assertions can verify the join matched the correct rows.
    lefty: {
      sql: `SELECT block_num, CAST(block_num AS BIGINT) * 10 + 1 AS left_val FROM anvil_rpc.blocks`,
      network: "anvil",
    },
    righty: {
      sql: `SELECT block_num, CAST(block_num AS BIGINT) * 10 + 2 AS right_val FROM anvil_rpc.blocks`,
      network: "anvil",
    },
  },
  functions: {},
}))
