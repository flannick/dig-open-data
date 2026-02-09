\name{cache_config}
\alias{cache_config}
\alias{CacheConfig}
\title{Configure caching}
\description{Create a cache configuration used to store downloaded files locally.}
\usage{
cache_config(dir, max_bytes = 10 * 1024^3, ttl_days = NULL)

CacheConfig(dir, max_bytes = 10 * 1024^3, ttl_days = NULL)
}
\arguments{
  \item{dir}{Cache directory.}
  \item{max_bytes}{Maximum cache size in bytes (default 10GB).}
  \item{ttl_days}{Optional time-to-live in days; NULL disables TTL.}
}
\value{A cache configuration object.}
\examples{
\dontrun{
cache <- cache_config("/data/dig_cache", max_bytes = 5 * 1024^3)
}
}
