test_that("parse_list_objects handles namespaced S3 XML", {
  xml <- charToRaw(paste0(
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    '<IsTruncated>true</IsTruncated>',
    '<NextContinuationToken>token-1</NextContinuationToken>',
    '<Contents><Key>bottom-line/EU/A.sumstats.tsv.gz</Key></Contents>',
    '<Contents><Key>bottom-line/EU/B.sumstats.tsv.gz</Key></Contents>',
    '<CommonPrefixes><Prefix>bottom-line/EU/</Prefix></CommonPrefixes>',
    '<CommonPrefixes><Prefix>bottom-line/AF/</Prefix></CommonPrefixes>',
    '</ListBucketResult>'
  ))

  result <- parse_list_objects(xml)

  expect_true(result$is_truncated)
  expect_equal(result$next_token, "token-1")
  expect_equal(result$keys, c(
    "bottom-line/EU/A.sumstats.tsv.gz",
    "bottom-line/EU/B.sumstats.tsv.gz"
  ))
  expect_equal(result$common_prefixes, c("bottom-line/EU/", "bottom-line/AF/"))
})

test_that("parse_list_objects handles missing continuation token and empty contents", {
  xml <- charToRaw(paste0(
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
    '<IsTruncated>false</IsTruncated>',
    '</ListBucketResult>'
  ))

  result <- parse_list_objects(xml)

  expect_false(result$is_truncated)
  expect_null(result$next_token)
  expect_equal(result$keys, character(0))
  expect_equal(result$common_prefixes, character(0))
})

test_that("build_query_string URL-encodes continuation tokens", {
  query <- list(
    `list-type` = "2",
    prefix = "bottom-line/EU/",
    `continuation-token` = "abc+def/ghi=="
  )

  result <- build_query_string(query)

  expect_true(grepl("prefix=bottom-line%2FEU%2F", result, fixed = TRUE))
  expect_true(grepl("continuation-token=abc%2Bdef%2Fghi%3D%3D", result, fixed = TRUE))
})
