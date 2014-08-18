#!/usr/bin/env Rscript
library("Imap")
f <- file("stdin")
open(f)
separator <- Sys.getenv(c("SEPARATOR"))
while(length(line <- readLines(f,n=1)) > 0) {
  # process line
  contents <- strsplit(line, separator)
  output = paste(contents, collapse=" ")
  write(output, stdout())
}