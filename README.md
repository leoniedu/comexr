
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
##installing arrow tips 
## https://arrow.apache.org/docs/r/articles/install.html#:~:text=Step%201%20-%20Using%20a%20computer%20with%20internet,created%20my_arrow_pkg.tar.gz%20to%20the%20computer%20without%20internet%20access



##devtools::install_github("leoniedu/comexstatr")
##install.packages("tictoc")
library(comexstatr)
library(dplyr)
```

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

``` r
##downloading
tictoc::tic()
comexstat_download(
  ##force_download = TRUE
  )
```

    ## Downloading data from Comexstat...

    ## ℹ Using "','" as decimal and "'.'" as grouping mark. Use `read_delim()` for more control.

    ## Rows: 26 Columns: 8
    ## ── Column specification ────────────────────────────────────────────────────────
    ## Delimiter: ";"
    ## chr (1): ARQUIVO
    ## dbl (7): CO_ANO, QT_ESTAT, KG_LIQUIDO, VL_FOB, VL_FRETE, VL_SEGURO, NUMERO_L...
    ## 
    ## ℹ Use `spec()` to retrieve the full column specification for this data.
    ## ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
    ## Downloading done!

``` r
## might need something like this if you get ssl errors. 
##comexstat_download(method="wget", extra="--no-check-certificate")
tictoc::toc()
```

    ## 0.63 sec elapsed

``` r
## summarise by product code (NCM) and year
tictoc::tic()
cstat <- comexstat()
cstat_ncm_year <- cstat|>
  filter(co_ano>=2017)|>
  group_by(co_ano, co_ncm, fluxo)|>
  summarise(vl_fob=sum(vl_fob))|>
  collect()|>
  arrange(co_ano, co_ncm, fluxo)
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

    ## 1.122 sec elapsed

Main trade partners.

``` r
cstat_top_0 <- cstat|>
  #filter(co_ano>=2017)|>
  group_by(co_pais)|>
  summarise(vl_fob=sum(vl_fob))|>
  ungroup() |> 
  arrange(desc(vl_fob))|>
  collect()|>
  slice(1:3)

cstat_top <- cstat |> 
  #filter(co_ano>=2017)%>%
  semi_join(cstat_top_0, by=c("co_pais"))%>%
  group_by(co_ano, co_pais, fluxo)|>
  summarise(vl_fob=sum(vl_fob))%>%
  collect%>%
  left_join(pais())
```

    ## Joining, by = "co_pais"

``` r
library(ggplot2)
qplot(co_ano, vl_fob_bi, data=cstat_top%>%filter(co_ano<2022)%>%mutate(vl_fob_bi=vl_fob/1e9), color=no_pais_ing, geom="line") +
  facet_wrap(~fluxo) +
  labs(color="", x="", y="US$ Bi (FOB)") +
  theme_linedraw() + theme(legend.position="bottom")
```

![](README_files/figure-gfm/unnamed-chunk-2-1.png)<!-- -->

## By ISIC - International Standard Industrial Classification of All Economic Activities

``` r
isic <- cstat |> 
  left_join(ncms()%>%select(co_ncm, isic=no_isic_secao_ing))%>%
  group_by(isic, co_ano, fluxo)%>%
  summarise(vl_fob=sum(vl_fob))%>%
  collect
```

    ## Joining, by = "co_cgce_n3"
    ## Joining, by = "co_cuci_item"
    ## Joining, by = "co_isic_classe"
    ## Joining, by = "co_unid"

``` r
qplot(co_ano, vl_fob_bi, 
      data=isic%>%mutate(vl_fob_bi=vl_fob/1e9), color=isic, geom="line") +
  facet_wrap(~fluxo) +
  labs(color="", x="", y="US$ Bi (FOB)") +
  theme_linedraw() + theme(legend.position="bottom")
```

![](README_files/figure-gfm/unnamed-chunk-3-1.png)<!-- -->
