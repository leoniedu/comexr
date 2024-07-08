# zzz.R
.onLoad <- function(libname, pkgname) {
    ## memoise saves computing time by storing results memoise from scratch while loading package
    ncms <<- memoise::memoise(ncms, cache = cachem::cache_disk(dir = comexr:::cdircomex))
    # ym <<- memoise::memoise(ym, cache = cachem::cache_disk(dir=comexr:::cdircomex))
    get_brlusd <<- memoise::memoise(get_brlusd, cache = cachem::cache_disk(dir = comexr:::cdircomex))
    get_deflators <<- memoise::memoise(get_deflators, cache = cachem::cache_disk(dir = comexr:::cdircomex))
}

