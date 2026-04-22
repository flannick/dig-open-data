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


test_that("is_remote_uri recognizes s3 URLs", {
  expect_true(is_remote_uri("s3://dig-open-bottom-line-analysis/bottom-line/EU/A.sumstats.tsv.gz"))
  expect_true(is_remote_uri("https://example.com/file.tsv.gz"))
  expect_false(is_remote_uri("/tmp/file.tsv.gz"))
})


test_that("open_local_connection returns a usable gzip connection", {
  path <- tempfile(fileext = ".gz")
  con <- gzfile(path, open = "wt")
  writeLines(c("a", "b"), con)
  close(con)

  con <- open_local_connection(path)
  on.exit(close(con), add = TRUE)

  expect_equal(readLines(con), c("a", "b"))
})
