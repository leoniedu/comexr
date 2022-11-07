
<!-- README.md is generated from README.Rmd. Please edit that file -->

# comexstatr

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![CRAN
status](https://www.r-pkg.org/badges/version/comexstatr)](https://CRAN.R-project.org/package=comexstatr)
<!-- badges: end -->

The goal of comexstatr is to make it easy to download, process, and
analyze Brazilian foreign trade statistics, available through the web
app <http://comexstat.mdic.gov.br/>, using the underlying bulk data
<https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta>.

## Installation

You can install the development version of comexstatr cloning the repo
from github and installing locally.

## Example

``` r
library(comexstatr)
#library(dplyr)
#library(tictoc)

##downloading
tictoc::tic()
comexstat_download_raw(
  ##force_download = TRUE
  )
```

    ## Downloading data from Comexstat...

``` r
tictoc::toc()
```

    ## 0.292 sec elapsed

``` r
## summarise by ncm and year
tictoc::tic()
cstat <- comexstat()
cstat_ncm_year <- cstat|>
  dplyr::filter(co_ano>=2017)|>
  dplyr::group_by(co_ano, co_ncm, fluxo)|>
  dplyr::summarise(vl_fob=sum(vl_fob))|>
  dplyr::collect()|>
  dplyr::arrange(co_ano, co_ncm, fluxo)
head(cstat_ncm_year)
```

    ## # A tibble: 6 × 4
    ## # Groups:   co_ano, co_ncm [3]
    ##   co_ano co_ncm   fluxo  vl_fob
    ##    <int> <chr>    <chr>   <dbl>
    ## 1   2017 01012100 exp   4546869
    ## 2   2017 01012100 imp   3028068
    ## 3   2017 01012900 exp   2657375
    ## 4   2017 01012900 imp   2753243
    ## 5   2017 01022110 exp    809930
    ## 6   2017 01022110 imp     39575

``` r
tictoc::toc()
```

    ## 7.719 sec elapsed

``` r
tictoc::tic()
cstat_year <- cstat|>
  dplyr::group_by(co_ano, fluxo)|>
  dplyr::summarise(vl_fob_bi=sum(vl_fob)/1e9)|>
  dplyr::collect()|>
  dplyr::arrange(co_ano, fluxo)
head(cstat_year)
```

    ## # A tibble: 6 × 3
    ## # Groups:   co_ano [3]
    ##   co_ano fluxo vl_fob_bi
    ##    <int> <chr>     <dbl>
    ## 1   1997 exp        52.9
    ## 2   1997 imp        60.5
    ## 3   1998 exp        51.1
    ## 4   1998 imp        58.7
    ## 5   1999 exp        47.9
    ## 6   1999 imp        50.3

``` r
tictoc::toc()
```

    ## 6.474 sec elapsed
