# zzz.R
.onLoad <- function(libname, pkgname) {
  #' @export
  ncms <<- memoise::memoise(ncms)
}
