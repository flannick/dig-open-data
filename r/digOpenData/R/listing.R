list_ancestries <- function(prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET, max_keys = 1000) {
  prefix <- ensure_prefix(prefix)
  result <- s3_list_all_objects(bucket = bucket, prefix = prefix, delimiter = "/", max_keys = max_keys)
  ancestry <- vapply(result$common_prefixes, function(p) tail(strsplit(trim_trailing(p, "/"), "/")[[1]], 1), character(1))
  sort(unique(ancestry))
}

list_files <- function(ancestry = NULL, prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET,
                       max_keys = 1000, limit = NULL, contains = NULL) {
  keys <- list_dataset_files(ancestry = ancestry, prefix = prefix, bucket = bucket,
                             max_keys = max_keys, limit = limit, contains = contains)
  keys
}

list_files_with_metadata <- function(ancestry = NULL, prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET,
                                     max_keys = 1000, limit = NULL, contains = NULL) {
  keys <- list_dataset_files(ancestry = ancestry, prefix = prefix, bucket = bucket,
                             max_keys = max_keys, limit = limit, contains = contains)
  entries <- lapply(keys, function(key) key_to_metadata(key, prefix, ancestry))
  entries <- do.call(rbind, lapply(entries, as.data.frame))
  if (!is.null(limit)) {
    entries <- entries[seq_len(min(limit, nrow(entries))), , drop = FALSE]
  }
  entries
}

list_traits <- function(ancestry = NULL, prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET,
                        max_keys = 1000, limit = NULL, contains = NULL) {
  entries <- list_files_with_metadata(ancestry = ancestry, prefix = prefix, bucket = bucket,
                                      max_keys = max_keys, limit = NULL, contains = contains)
  traits <- unique(na.omit(entries$trait))
  traits <- sort(traits)
  if (!is.null(limit)) {
    traits <- traits[seq_len(min(limit, length(traits)))]
  }
  traits
}

list_dataset_files <- function(ancestry = NULL, prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET,
                               max_keys = 1000, limit = NULL, contains = NULL) {
  prefix <- ensure_prefix(prefix)
  if (!is.null(ancestry)) {
    prefix <- paste0(prefix, trim_slash(ancestry), "/")
  }
  result <- s3_list_all_objects(bucket = bucket, prefix = prefix, delimiter = NULL, max_keys = max_keys)
  keys <- sort(unique(result$keys))
  if (!is.null(contains)) {
    keys <- keys[grepl(contains, keys, fixed = TRUE)]
  }
  if (!is.null(limit)) {
    keys <- keys[seq_len(min(limit, length(keys)))]
  }
  keys
}

key_to_metadata <- function(key, prefix, ancestry_override = NULL) {
  ancestry <- ancestry_override
  if (is.null(ancestry)) {
    ancestry <- extract_ancestry_from_key(key, DEFAULT_PREFIX)
    if (is.null(ancestry)) {
      ancestry <- extract_ancestry_from_key(key, ensure_prefix(prefix))
    }
    if (is.null(ancestry)) {
      ancestry <- extract_ancestry_from_prefix(prefix, DEFAULT_PREFIX)
    }
  }
  filename <- basename(key)
  trait <- NA_character_
  if (endsWith(filename, DEFAULT_SUFFIX)) {
    trait <- substr(filename, 1, nchar(filename) - nchar(DEFAULT_SUFFIX))
  }
  data.frame(
    ancestry = if (is.null(ancestry)) NA_character_ else ancestry,
    trait = trait,
    filename = filename,
    key = key,
    stringsAsFactors = FALSE
  )
}

ensure_prefix <- function(prefix) {
  if (prefix == "") return(prefix)
  if (endsWith(prefix, "/")) return(prefix)
  paste0(prefix, "/")
}

trim_trailing <- function(text, suffix) {
  if (endsWith(text, suffix)) {
    substr(text, 1, nchar(text) - nchar(suffix))
  } else {
    text
  }
}

trim_slash <- function(text) {
  gsub("^/+|/+$", "", text)
}

extract_ancestry_from_key <- function(key, base_prefix) {
  if (!startsWith(key, base_prefix)) return(NULL)
  remainder <- substr(key, nchar(base_prefix) + 1, nchar(key))
  if (!grepl("/", remainder, fixed = TRUE)) return(NULL)
  strsplit(remainder, "/", fixed = TRUE)[[1]][1]
}

extract_ancestry_from_prefix <- function(prefix, base_prefix) {
  base_prefix <- ensure_prefix(base_prefix)
  prefix <- ensure_prefix(prefix)
  if (!startsWith(prefix, base_prefix)) return(NULL)
  remainder <- trim_slash(substr(prefix, nchar(base_prefix) + 1, nchar(prefix)))
  if (remainder == "") return(NULL)
  strsplit(remainder, "/", fixed = TRUE)[[1]][1]
}
