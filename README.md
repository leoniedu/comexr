
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
  #force_download = TRUE
  )
```

    ## Downloading data from Comexstat...

``` r
tictoc::toc()
```

    ## 0.285 sec elapsed

``` r
## summarise by ncm and year
tictoc::tic()
cstat <- comexstat_raw()
```

    ## 0.327 sec elapsed

``` r
cstat_ncm_year <- cstat|>
  dplyr::filter(co_ano==2021)|>
  dplyr::group_by(co_ano, co_ncm, fluxo)|>
  dplyr::summarise(vl_fob=sum(vl_fob))|>
  dplyr::collect()|>
  dplyr::arrange(co_ano, co_ncm, fluxo)
head(cstat_ncm_year)
```

    ## # A tibble: 6 × 4
    ## # Groups:   co_ano, co_ncm [4]
    ##   co_ano co_ncm   fluxo  vl_fob
    ##    <int> <chr>    <chr>   <dbl>
    ## 1   2021 01012100 exp   1894729
    ## 2   2021 01012100 imp   2290284
    ## 3   2021 01012900 exp   5230872
    ## 4   2021 01012900 imp   1960884
    ## 5   2021 01019000 exp       566
    ## 6   2021 01022110 exp     25606

``` r
tictoc::toc()
```

    ## 6.993 sec elapsed
