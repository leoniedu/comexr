# zzz.R
.onLoad <- function(libname, pkgname) {
  ## memoise saves computing time by storing results
  ## memoise from scratch while loading package
  ncms <<- memoise::memoise(ncms)
  ym <<- memoise::memoise(ym)
}

