# zzz.R
.onLoad <- function(libname, pkgname) {
  ## memoise saves computing time by storing results
  ## memoise from scratch while loading package
  ncms <<- memoise::memoise(ncms)
  ym <<- memoise::memoise(ym)
  get_brlusd <- memoise::memoise(get_brlusd)
  get_deflators <<- memoise::memoise(get_deflators)
}

