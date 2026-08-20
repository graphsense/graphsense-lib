package org.graphsense.account.trx

import java.nio.file.Files
import org.graphsense.TestBase
import org.graphsense.TransformHelpers

class BlockRangeToProcessTest extends TestBase {

  import spark.implicits._

  test(
    "blockRangeToProcess: the first resolved range is pinned across reruns"
  ) {
    val cacheDir = Files.createTempDirectory("gs-cache-test").toString

    // First run resolves the range from its (fresh) exchange-rate horizon.
    val first = TransformHelpers
      .computeCached(Some(cacheDir), spark)("blockRangeToProcess") {
        Seq(BlockRangeToProcess(0, 100)).toDS()
      }
      .first()
    assert(first == BlockRangeToProcess(0, 100))

    // A rerun sees a newer rate horizon (maxBlock 200), but must get the
    // pinned range back from the cache — the compute block must not run.
    val rerun = TransformHelpers
      .computeCached(Some(cacheDir), spark)("blockRangeToProcess") {
        fail("compute block ran despite an existing cached range")
        Seq(BlockRangeToProcess(0, 200)).toDS()
      }
      .first()
    assert(rerun == BlockRangeToProcess(0, 100))
  }

  test("blockRangeToProcess: no cache directory means no pinning") {
    val range = TransformHelpers
      .computeCached(None, spark)("blockRangeToProcess") {
        Seq(BlockRangeToProcess(5, 42)).toDS()
      }
      .first()
    assert(range == BlockRangeToProcess(5, 42))
  }

  test(
    "cachePinnedConfig: a rerun under changed arguments sees the pinned config"
  ) {
    val cacheDir = Files.createTempDirectory("gs-cache-test").toString
    // the 2026-07/08 incident shape: cache built with bucket size 450000 …
    val builtWith = CachePinnedConfig(25000, 450000, 100, 5, 5)
    TransformHelpers
      .computeCached(Some(cacheDir), spark)("cachePinnedConfig") {
        Seq(builtWith).toDS()
      }
      .first()

    // … then the argument is changed to 50000: the pinned row must surface
    // the mismatch (the job compares and refuses to run).
    val current = builtWith.copy(blockBucketSizeAddressTxs = 50000)
    val pinned = TransformHelpers
      .computeCached(Some(cacheDir), spark)("cachePinnedConfig") {
        Seq(current).toDS()
      }
      .first()
    assert(pinned == builtWith)
    assert(pinned != current)
  }
}
