DEFAULT_BUCKET <- "dig-open-bottom-line-analysis"
DEFAULT_PREFIX <- "bottom-line/"
DEFAULT_SUFFIX <- ".sumstats.tsv.gz"

s3_urls <- function(bucket, key) {
  quoted <- utils::URLencode(key, reserved = TRUE)
  if (quoted != "") {
    return(c(
      sprintf("https://%s.s3.amazonaws.com/%s", bucket, quoted),
      sprintf("https://s3.amazonaws.com/%s/%s", bucket, quoted),
      sprintf("https://%s.s3.us-east-1.amazonaws.com/%s", bucket, quoted),
      sprintf("https://s3.us-east-1.amazonaws.com/%s/%s", bucket, quoted)
    ))
  }
  c(
    sprintf("https://%s.s3.amazonaws.com/", bucket),
    sprintf("https://s3.amazonaws.com/%s/", bucket),
    sprintf("https://%s.s3.us-east-1.amazonaws.com/", bucket),
    sprintf("https://s3.us-east-1.amazonaws.com/%s/", bucket)
  )
}

s3_list_objects_page <- function(bucket, prefix = "", delimiter = NULL, max_keys = 1000, token = NULL) {
  query <- list(`list-type` = "2", `max-keys` = as.character(max_keys))
  if (prefix != "") query$prefix <- prefix
  if (!is.null(delimiter)) query$delimiter <- delimiter
  if (!is.null(token)) query$`continuation-token` <- token
  query_string <- paste(paste(names(query), query, sep = "="), collapse = "&")

  url <- sprintf("https://%s.s3.amazonaws.com?%s", bucket, query_string)
  con <- url(url, open = "rb")
  on.exit(close(con), add = TRUE)
  xml <- readBin(con, "raw", n = 1e7)
  parse_list_objects(xml)
}

parse_list_objects <- function(xml_raw) {
  doc <- xml2::read_xml(xml_raw)
  is_truncated <- xml2::xml_text(xml2::xml_find_first(doc, ".//{*}IsTruncated")) == "true"
  next_token <- xml2::xml_text(xml2::xml_find_first(doc, ".//{*}NextContinuationToken"))
  if (is.na(next_token)) next_token <- NULL

  keys <- xml2::xml_find_all(doc, ".//{*}Contents/{*}Key")
  keys <- xml2::xml_text(keys)

  prefixes <- xml2::xml_find_all(doc, ".//{*}CommonPrefixes/{*}Prefix")
  prefixes <- xml2::xml_text(prefixes)

  list(keys = keys, common_prefixes = prefixes, is_truncated = is_truncated, next_token = next_token)
}

s3_list_all_objects <- function(bucket, prefix = "", delimiter = NULL, max_keys = 1000) {
  keys <- character(0)
  prefixes <- character(0)
  token <- NULL
  repeat {
    page <- s3_list_objects_page(bucket = bucket, prefix = prefix, delimiter = delimiter, max_keys = max_keys, token = token)
    keys <- c(keys, page$keys)
    prefixes <- c(prefixes, page$common_prefixes)
    if (!isTRUE(page$is_truncated)) {
      return(list(keys = keys, common_prefixes = prefixes))
    }
    token <- page$next_token
  }
}
