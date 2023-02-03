# zzz.R
.onLoad <- function(libname, pkgname) {
  ncms <<- memoise::memoise(ncms)
  ym <<- memoise::memoise(ym)
  comexstat <<- memoise::memoise(comexstat)
}

