\name{list_ancestries}
\alias{list_ancestries}
\alias{list_traits}
\alias{list_files}
\alias{list_files_with_metadata}
\title{List DIG Open Data ancestries, traits, and files}
\description{List available ancestries, trait names, and file keys from the DIG Open Data bucket.}
\usage{
list_ancestries(prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET, max_keys = 1000)

list_traits(ancestry = NULL, prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET,
            max_keys = 1000, limit = NULL, contains = NULL)

list_files(ancestry = NULL, prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET,
           max_keys = 1000, limit = NULL, contains = NULL)

list_files_with_metadata(ancestry = NULL, prefix = DEFAULT_PREFIX, bucket = DEFAULT_BUCKET,
                         max_keys = 1000, limit = NULL, contains = NULL)
}
\arguments{
  \item{ancestry}{Optional ancestry code to filter results.}
  \item{prefix}{Bucket prefix used for listings.}
  \item{bucket}{S3 bucket name.}
  \item{max_keys}{Maximum keys per S3 request (pagination handled automatically).}
  \item{limit}{Optional limit on number of results returned.}
  \item{contains}{Optional substring filter.}
}
\value{
  \code{list_ancestries} returns a character vector of ancestry labels.
  \code{list_traits} returns a character vector of trait names.
  \code{list_files} returns a character vector of object keys.
  \code{list_files_with_metadata} returns a data.frame with ancestry, trait, filename, and key.
}
\examples{
\dontrun{
list_ancestries()
list_traits(ancestry = "EU")
list_files(contains = "T2D")
list_files_with_metadata(ancestry = "EU")
}
}
