open_trait <- function(ancestry, trait, bucket = DEFAULT_BUCKET, prefix = DEFAULT_PREFIX,
                       suffix = DEFAULT_SUFFIX, encoding = "UTF-8", retries = 3,
                       download = FALSE, cache = NULL) {
  key <- build_key(ancestry, trait, prefix = prefix, suffix = suffix)
  open_file_key(key, bucket = bucket, encoding = encoding, retries = retries,
                download = download, cache = cache)
}

open_file_key <- function(key, bucket = DEFAULT_BUCKET, encoding = "UTF-8", retries = 3,
                          download = FALSE, cache = NULL) {
  uri <- sprintf("s3://%s/%s", bucket, key)
  open_text(uri, encoding = encoding, retries = retries, download = download, cache = cache)
}

build_key <- function(ancestry, trait, prefix = DEFAULT_PREFIX, suffix = DEFAULT_SUFFIX) {
  trait_clean <- trait
  if (endsWith(trait_clean, suffix)) {
    trait_clean <- substr(trait_clean, 1, nchar(trait_clean) - nchar(suffix))
  }
  paste0(ensure_prefix(prefix), trim_slash(ancestry), "/", trait_clean, suffix)
}

open_text <- function(uri, encoding = "UTF-8", retries = 3, download = FALSE, cache = NULL) {
  cache_cfg <- cache
  if (is.null(cache_cfg)) {
    cache_cfg <- cache_config_from_env()
  }
  if (!is.null(cache_cfg) && is_remote_uri(uri)) {
    return(open_text_cached(uri, encoding = encoding, retries = retries, cache = cache_cfg))
  }
  if (download && is_remote_uri(uri)) {
    return(open_text_downloaded(uri, encoding = encoding, retries = retries))
  }
  open_connection(uri, encoding = encoding)
}

read_lines <- function(uri, encoding = "UTF-8", retries = 3, download = FALSE, cache = NULL) {
  lines <- character(0)
  remaining <- retries
  repeat {
    con <- open_text(uri, encoding = encoding, retries = 0, download = download, cache = cache)
    on.exit(close(con), add = TRUE)
    if (length(lines) > 0) {
      suppressWarnings(readLines(con, n = length(lines)))
    }
    result <- tryCatch(readLines(con), error = function(e) e)
    close(con)
    if (inherits(result, "error")) {
      if (remaining <= 0) stop(result)
      remaining <- remaining - 1
      next
    }
    lines <- c(lines, result)
    return(lines)
  }
}

read_trait_lines <- function(ancestry, trait, bucket = DEFAULT_BUCKET, prefix = DEFAULT_PREFIX,
                             suffix = DEFAULT_SUFFIX, encoding = "UTF-8", retries = 3,
                             download = FALSE, cache = NULL) {
  con <- open_trait(ancestry, trait, bucket = bucket, prefix = prefix, suffix = suffix,
                    encoding = encoding, retries = retries, download = download, cache = cache)
  on.exit(close(con), add = TRUE)
  readLines(con)
}

read_file_lines <- function(key, bucket = DEFAULT_BUCKET, encoding = "UTF-8",
                            retries = 3, download = FALSE, cache = NULL) {
  con <- open_file_key(key, bucket = bucket, encoding = encoding, retries = retries,
                       download = download, cache = cache)
  on.exit(close(con), add = TRUE)
  readLines(con)
}

open_connection <- function(uri, encoding = "UTF-8") {
  if (is_remote_uri(uri)) {
    return(open_remote_connection(uri, encoding = encoding))
  }
  open_local_connection(uri, encoding = encoding)
}

open_local_connection <- function(path, encoding = "UTF-8") {
  con <- file(path, open = "rb")
  on.exit(close(con), add = TRUE)
  gz <- is_gzipped(con)
  close(con)
  con <- file(path, open = "rb")
  if (gz) {
    gzcon(con, encoding = encoding)
  } else {
    con
  }
}

open_remote_connection <- function(uri, encoding = "UTF-8") {
  parsed <- parse_uri(uri)
  if (parsed$scheme == "s3") {
    return(open_s3_connection(parsed$bucket, parsed$key, encoding = encoding))
  }
  con <- url(uri, open = "rb")
  on.exit(close(con), add = TRUE)
  gz <- is_gzipped(con)
  close(con)
  con <- url(uri, open = "rb")
  if (gz) {
    gzcon(con, encoding = encoding)
  } else {
    con
  }
}

open_s3_connection <- function(bucket, key, encoding = "UTF-8") {
  urls <- s3_urls(bucket, key)
  last_error <- NULL
  for (url in urls) {
    con <- tryCatch(url(url, open = "rb"), error = function(e) e)
    if (inherits(con, "error")) {
      last_error <- con
      next
    }
    on.exit(close(con), add = TRUE)
    gz <- tryCatch(is_gzipped(con), error = function(e) e)
    close(con)
    if (inherits(gz, "error")) {
      last_error <- gz
      next
    }
    con <- url(url, open = "rb")
    if (isTRUE(gz)) {
      return(gzcon(con, encoding = encoding))
    }
    return(con)
  }
  stop(last_error)
}

is_gzipped <- function(con) {
  bytes <- readBin(con, "raw", n = 2)
  identical(bytes, as.raw(c(0x1f, 0x8b)))
}

parse_uri <- function(uri) {
  if (startsWith(uri, "s3://")) {
    no_scheme <- sub("^s3://", "", uri)
    parts <- strsplit(no_scheme, "/", fixed = TRUE)[[1]]
    bucket <- parts[1]
    key <- paste(parts[-1], collapse = "/")
    return(list(scheme = "s3", bucket = bucket, key = key))
  }
  list(scheme = "file")
}

is_remote_uri <- function(uri) {
  grepl("^[a-zA-Z]+://", uri)
}

open_text_downloaded <- function(uri, encoding = "UTF-8", retries = 3) {
  path <- download_with_retries(uri, retries = retries)
  open_local_connection(path, encoding = encoding)
}

open_text_cached <- function(uri, encoding = "UTF-8", retries = 3, cache) {
  store <- cache_store(cache)
  cached <- cache_get(store, uri)
  if (!is.null(cached)) {
    return(open_local_connection(cached, encoding = encoding))
  }
  tmp <- download_with_retries(uri, retries = retries, return_size = TRUE)
  cached_path <- cache_put(store, uri, tmp$path, tmp$size)
  open_local_connection(cached_path, encoding = encoding)
}

download_with_retries <- function(uri, retries = 3, return_size = FALSE) {
  attempts <- max(0, retries)
  last_error <- NULL
  for (i in seq_len(attempts + 1)) {
    result <- tryCatch(download_to_temp(uri), error = function(e) e)
    if (!inherits(result, "error")) {
      if (return_size) return(result)
      return(result$path)
    }
    last_error <- result
  }
  stop(last_error)
}

download_to_temp <- function(uri) {
  con <- open_connection(uri)
  on.exit(close(con), add = TRUE)
  path <- tempfile(pattern = "dig-open-data-")
  out <- file(path, open = "wb")
  on.exit(close(out), add = TRUE)
  size <- 0
  repeat {
    chunk <- readBin(con, "raw", n = 1024 * 1024)
    if (length(chunk) == 0) break
    writeBin(chunk, out)
    size <- size + length(chunk)
  }
  list(path = path, size = size)
}
