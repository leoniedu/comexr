# zzz.R
.onLoad <- function(libname, pkgname) {
  ncms <<- memoise::memoise(ncms)
  read_comex <<- memoise::memoise(read_comex)
  ym <<- memoise::memoise(ym)
}

