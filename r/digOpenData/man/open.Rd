\name{open_trait}
\alias{open_trait}
\alias{read_lines}
\alias{build_key}
\title{Open and read DIG Open Data traits}
\description{Open trait files by ancestry and trait name, or read all lines with retries.}
\usage{
open_trait(ancestry, trait, bucket = DEFAULT_BUCKET, prefix = DEFAULT_PREFIX,
           suffix = DEFAULT_SUFFIX, encoding = "UTF-8", retries = 3,
           download = FALSE, cache = NULL)

read_lines(uri, encoding = "UTF-8", retries = 3, download = FALSE, cache = NULL)

build_key(ancestry, trait, prefix = DEFAULT_PREFIX, suffix = DEFAULT_SUFFIX)
}
\arguments{
  \item{ancestry}{Ancestry code.}
  \item{trait}{Trait name (without suffix).}
  \item{bucket}{S3 bucket name.}
  \item{prefix}{Bucket prefix.}
  \item{suffix}{File suffix for trait files.}
  \item{encoding}{Text encoding.}
  \item{retries}{Number of retry attempts on read errors.}
  \item{download}{If TRUE, download the file before reading.}
  \item{cache}{Optional cache configuration object.}
  \item{uri}{URI or local path.}
}
\value{
  \code{open_trait} returns a readable connection.
  \code{read_lines} returns a character vector of lines.
  \code{build_key} returns the object key for a trait.
}
\examples{
\dontrun{
con <- open_trait("EU", "AlbInT2D")
lines <- read_lines("s3://dig-open-bottom-line-analysis/bottom-line/EU/AlbInT2D.sumstats.tsv.gz")
key <- build_key("EU", "AlbInT2D")
}
}
