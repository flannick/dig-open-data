library(testthat)


test_that("build_key handles suffix", {
  key <- build_key("EU", "Trait1")
  expect_true(grepl("Trait1.sumstats.tsv.gz$", key))

  key2 <- build_key("EU", "Trait1.sumstats.tsv.gz")
  expect_equal(key, key2)
})


test_that("cache_config creates object", {
  cache <- cache_config("/tmp/dig_cache", max_bytes = 123, ttl_days = 7)
  expect_true(inherits(cache, "dig_cache_config"))
  expect_equal(cache$max_bytes, 123)
})
