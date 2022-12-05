# zzz.R
.onLoad <- function(libname, pkgname) {
  ncms <<- memoise::memoise(ncms)
}
