CacheConfig <- function(dir, max_bytes = 10 * 1024^3, ttl_days = NULL) {
  structure(
    list(dir = dir, max_bytes = max_bytes, ttl_days = ttl_days),
    class = "dig_cache_config"
  )
}

cache_config <- function(dir, max_bytes = 10 * 1024^3, ttl_days = NULL) {
  CacheConfig(dir = dir, max_bytes = max_bytes, ttl_days = ttl_days)
}

cache_config_from_env <- function() {
  cache_dir <- Sys.getenv("DIG_OPEN_DATA_CACHE_DIR", unset = "")
  if (cache_dir == "") {
    return(NULL)
  }
  max_bytes <- parse_int_env("DIG_OPEN_DATA_CACHE_MAX_BYTES", 10 * 1024^3)
  ttl_days <- parse_int_env("DIG_OPEN_DATA_CACHE_TTL_DAYS", NA_integer_)
  if (is.na(ttl_days)) {
    ttl_days <- NULL
  }
  CacheConfig(dir = cache_dir, max_bytes = max_bytes, ttl_days = ttl_days)
}

cache_store <- function(config) {
  dir.create(file.path(config$dir, "objects"), recursive = TRUE, showWarnings = FALSE)
  list(
    config = config,
    objects_dir = file.path(config$dir, "objects"),
    index_path = file.path(config$dir, "index.rds")
  )
}

cache_get <- function(store, key) {
  index <- cache_load_index(store)
  entry <- index[[key]]
  if (is.null(entry)) {
    return(NULL)
  }
  if (!file.exists(entry$path)) {
    cache_delete_entry(store, key, entry)
    return(NULL)
  }
  if (cache_expired(store, entry)) {
    cache_delete_entry(store, key, entry)
    return(NULL)
  }
  cache_touch(store, key, entry)
  entry$path
}

cache_put <- function(store, key, source_path, size) {
  digest <- digest_key(key)
  dest_path <- file.path(store$objects_dir, digest)
  file.rename(source_path, dest_path)
  now <- as.integer(Sys.time())
  entry <- list(
    path = dest_path,
    size = size,
    created_at = now,
    last_access = now
  )
  index <- cache_load_index(store)
  index[[key]] <- entry
  cache_write_index(store, index)
  cache_evict(store, index)
  dest_path
}

cache_touch <- function(store, key, entry) {
  entry$last_access <- as.integer(Sys.time())
  index <- cache_load_index(store)
  index[[key]] <- entry
  cache_write_index(store, index)
}

cache_expired <- function(store, entry) {
  ttl_days <- store$config$ttl_days
  if (is.null(ttl_days)) {
    return(FALSE)
  }
  ttl_seconds <- ttl_days * 24 * 60 * 60
  elapsed <- as.integer(Sys.time()) - entry$last_access
  elapsed > ttl_seconds
}

cache_evict <- function(store, index) {
  max_bytes <- max(0, as.numeric(store$config$max_bytes))
  sizes <- vapply(index, function(entry) entry$size, numeric(1))
  total <- sum(sizes)
  if (total <= max_bytes) {
    return(invisible(NULL))
  }
  access_times <- vapply(index, function(entry) entry$last_access, numeric(1))
  keys <- names(sort(access_times, decreasing = FALSE))
  for (key in keys) {
    entry <- index[[key]]
    cache_delete_entry(store, key, entry)
    index <- cache_load_index(store)
    sizes <- vapply(index, function(entry) entry$size, numeric(1))
    total <- sum(sizes)
    if (total <= max_bytes) {
      break
    }
  }
}

cache_delete_entry <- function(store, key, entry) {
  if (!is.null(entry$path) && file.exists(entry$path)) {
    try(unlink(entry$path), silent = TRUE)
  }
  index <- cache_load_index(store)
  index[[key]] <- NULL
  cache_write_index(store, index)
}

cache_load_index <- function(store) {
  if (!file.exists(store$index_path)) {
    return(list())
  }
  readRDS(store$index_path)
}

cache_write_index <- function(store, index) {
  tmp <- tempfile(pattern = "dig-open-data-index-")
  saveRDS(index, tmp)
  file.rename(tmp, store$index_path)
}

parse_int_env <- function(name, default) {
  value <- Sys.getenv(name, unset = NA_character_)
  if (is.na(value) || value == "") {
    return(default)
  }
  parsed <- suppressWarnings(as.numeric(value))
  if (is.na(parsed)) {
    default
  } else {
    parsed
  }
}

digest_key <- function(key) {
  tmp <- tempfile(pattern = "dig-open-data-key-")
  writeLines(key, tmp)
  hex <- as.character(tools::md5sum(tmp))
  unlink(tmp)
  hex
}
