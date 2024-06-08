
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

``` r
##devtools::install_github("leoniedu/comexstatr")
```

If you have problems installing arrow, see:

<https://arrow.apache.org/docs/r/articles/install.html>

## Examples

``` r
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

try(comexstat_download2(years = 1997:2024))
```

Automatic downloading can be tricky, due to timeout, (lack of) valid
security certificates on the Brazilian government websites, along other
issues. The code uses the `multi_download` function from the `curl`
library, so it resumes download if it fails.

### Main trade partners, treating countries in Mercosul and European Union as blocks.

Using a programming language like R makes it easy to generate statistics
and reports at the intended level of analysis.

``` r
msul <- comexstat("pais_bloco")|>
  filter(block_code==111)|>
  pull(country_code)
eu <- comexstat("pais_bloco")|>
  filter(block_code==22)|>
  pull(country_code)

pb <- comexstat("pais")|>
  transmute(country_code, 
            partner=
              case_when(country_code%in%msul ~ "Mercosul",
                        country_code%in%eu ~ "European Union",
                        TRUE ~ country_name)
              )

cstat_top_0 <- comexstat_ncm()|>
  left_join(pb) |> 
  #filter(co_ano>=2017)|>
  group_by(partner)|>
  summarise(fob_usd=sum(fob_usd))|>
  ungroup() |> 
  arrange(desc(fob_usd))|>
  collect()|>
  slice(1:5)

cstat_top <- comexstat_ncm() |>
  left_join(pb) |> 
  #filter(co_ano>=2017)|>
  semi_join(cstat_top_0, by=c("partner"))|>
  group_by(year, partner, direction)|>
  summarise(fob_usd=sum(fob_usd))|>
  collect()

library(ggplot2)
ggplot(aes(x=year, 
           y=fob_usd_bi), 
       data=cstat_top|>
         filter(year<=2023)|>
         mutate(fob_usd_bi=fob_usd/1e9)) +
  geom_line(aes(color=partner)) +
  facet_wrap(~direction) +
  labs(color="", x="", y="US$ Bi (FOB)") +
  theme_linedraw() + theme(legend.position="bottom")
```

![](README_files/figure-gfm/topblocks-1.png)<!-- -->

### Imports and exports by Brazilian state

You will have access to information not available via the web interface
<http://comexstat.mdic.gov.br/en/home>, such as

``` r
bystate <- comexstat_ncm() |> 
  filter(year<=2023) |>
  group_by(state_abb, year, direction)|>
  summarise(fob_usd=sum(fob_usd))|>
  collect()

topstate <- bystate|>
  group_by(state_abb)|>
  summarise(fob_usd=sum(fob_usd))|>
  arrange(-fob_usd)|>
  head(3)


ggplot(aes(x=year, y=fob_usd_bi, color=state_abb), 
       data=bystate|>
        semi_join(topstate, by="state_abb")|>
        mutate(fob_usd_bi=fob_usd/1e9)) +
  geom_line() +
  facet_wrap(~direction) +
  labs(color="", x="", y="US$ Bi (FOB)") +
  theme_linedraw() + theme(legend.position="bottom")
```

![](README_files/figure-gfm/bystate-1.png)<!-- -->

## Deflate using CPI (for USD) or IPCA (for BRL) (Experimental)

``` r
selected_deflated <- comexstat_ncm()%>%
  filter(country_code%in%c(249, 160, 63))%>%
  group_by(direction, date, country_code)%>%
  summarise(fob_usd=sum(fob_usd), cif_usd=sum(cif_usd, na.rm=TRUE))%>%
  comexstat_deflated()%>%
  collect()

library(runner)
selected_deflated_r <- selected_deflated%>%
  left_join(comexstat("pais"))%>%
  group_by(direction, country_name)%>%
  arrange(date)%>%
  filter(!is.na(fob_usd))%>%
  mutate(fob_usd_constant_bi=
           slider::slide_index_dbl(.x=fob_usd_constant, 
                                   .before = months(11),
                                   .complete = TRUE,
                                   .f = function(z) sum(z, na.rm=TRUE), .i = date)/1e9)
```

    ## Joining with `by = join_by(country_code)`

``` r
ggplot(aes(x=date, y=fob_usd_constant_bi, color=country_name), 
       data=selected_deflated_r)+
  facet_wrap(~direction)+
  geom_line() +
  labs(color="", x="", y="US$ Bi (FOB) Deflated by CPI "%>%paste0(format(max(selected_deflated_r$date), "%m/%Y")), caption = "* 12 month rolling sums") +
  theme_linedraw() + theme(legend.position="bottom") #+ scale_color_manual(values=c("red",  "blue")) 
```

    ## Warning: Removed 33 rows containing missing values (`geom_line()`).

![](README_files/figure-gfm/deflated-1.png)<!-- -->

## Trade balance

``` r
balance_deflated <- comexstat_ncm()%>%
  group_by(direction, date)%>%
  summarise(fob_usd=sum(fob_usd), cif_usd=sum(cif_usd, na.rm=TRUE), qt_stat=sum(qt_stat, na.rm=TRUE))%>%
  comexstat_deflated()%>%
  collect()

library(runner)
nperiods <- 11
balance_deflated_r <- balance_deflated%>%
  group_by(direction)%>%
  arrange(date)%>%
  filter(!is.na(fob_usd_constant))%>%
  mutate(
    fob_usd_bi=
      slider::slide_index_dbl(.x=fob_usd, 
                              .before = months(nperiods),
                              .complete = TRUE,
                              .f = function(z) sum(z, na.rm=TRUE), .i = date)/1e9,
    fob_brl_bi=
      slider::slide_index_dbl(.x=fob_usd*brlusd, 
                              .before = months(nperiods),
                              .complete = TRUE,
                              .f = function(z) sum(z, na.rm=TRUE), .i = date)/1e9,
    fob_usd_constant_bi=
      slider::slide_index_dbl(.x=fob_usd_constant, 
                              .before = months(nperiods),
                              .complete = TRUE,
                              .f = function(z) sum(z, na.rm=TRUE), .i = date)/1e9,
    fob_brl_constant_bi=
      slider::slide_index_dbl(.x=fob_brl_constant, 
                              .before = months(nperiods),
                              .complete = TRUE,
                              .f = function(z) sum(z, na.rm=TRUE), .i = date)/1e9
  )%>%
  mutate(fob_usd_constant_bi_i=fob_usd_constant_bi/fob_usd_constant_bi[date==as.Date("2022-01-01")])

volume_deflated_r <- balance_deflated_r%>%
  group_by(date)%>%
  summarise(across(matches("^(fob|cif|qt)"), sum))

ggplot(aes(x=date, y=fob_usd_constant_bi, color=direction), 
       data=balance_deflated_r) +
  scale_color_manual(values=c("blue", "red")) +
  geom_line() +
  labs(color="", x="", y="US$ Bi (FOB) Deflated by CPI "%>%paste0(format(max(selected_deflated_r$date), "%m/%Y")), caption = "* 12 month rolling sums") +
  theme_linedraw() + 
  geom_vline(xintercept=as.Date("2023-01-01"))+
  theme(legend.position="bottom") #+ scale_color_manual(values=c("red",  "blue")) 
```

    ## Warning: Removed 22 rows containing missing values (`geom_line()`).

![](README_files/figure-gfm/deflated2-1.png)<!-- -->

``` r
ggplot(aes(x=date, y=fob_brl_constant_bi, color=direction), 
       data=balance_deflated_r) +
  scale_color_manual(values=c("blue", "red")) +
  geom_line() +
  #geom_line(aes(y=vl_fob_usd_bi), linetype='dashed')+
  labs(color="", x="", y="R$ Bi (FOB) Deflated by IPCA "%>%paste0(format(max(selected_deflated_r$date), "%m/%Y")), caption = "* 12 month rolling sums") +
  theme_linedraw() + 
  geom_vline(xintercept=as.Date("2024-01-01"))+
  theme(legend.position="bottom") #+ scale_color_manual(values=c("red",  "blue")) 
```

    ## Warning: Removed 22 rows containing missing values (`geom_line()`).

![](README_files/figure-gfm/unnamed-chunk-1-1.png)<!-- -->

### Somente último mês

``` r
ggplot(aes(x=date, y=fob_brl_constant_bi, color=direction), 
       data=balance_deflated_r%>%filter(lubridate::month(date)==lubridate::month(max(balance_deflated_r$date)))
       ) +
  scale_color_manual(values=c("blue", "red")) +
  geom_line() +
  geom_line(aes(y=fob_brl_bi), linetype='dashed') +
  #geom_point(aes(y=fob_brl_bi), linetype='dashed') +
  labs(color="", x="", y="R$ Bi (FOB) Deflated by IPCA "%>%paste0(format(max(selected_deflated_r$date), "%m/%Y")), caption = "* 12 month rolling sums") +
  theme_linedraw() + 
  geom_vline(xintercept=as.Date("2024-01-01"))+
  theme(legend.position="bottom") #+ scale_color_manual(values=c("red",  "blue")) 
```

    ## Warning: Removed 2 rows containing missing values (`geom_line()`).
    ## Removed 2 rows containing missing values (`geom_line()`).

![](README_files/figure-gfm/unnamed-chunk-2-1.png)<!-- -->

``` r
ggplot(aes(x=date, y=balance_usd_constant), 
       data=
         balance_deflated_r%>%
         group_by(date)%>%
         arrange(desc(direction))%>%
         summarise(balance_usd_constant=fob_usd_constant_bi[2]-fob_usd_constant_bi[1])%>%
         na.omit()
                   )+
  #scale_color_manual(values=c("blue", "red")) +
  geom_line()  +
  labs(color="", x="", y="USD$ Bi (FOB) Deflated by CPI "%>%paste0(format(max(selected_deflated_r$date), "%m/%Y")), caption = "* 12 month rolling sums") +
  theme_linedraw() + 
  theme(legend.position="bottom") #+ scale_color_manual(values=c("red",  "blue")) 
```

![](README_files/figure-gfm/unnamed-chunk-3-1.png)<!-- -->

### BRL

``` r
ggplot(aes(x=date, y=balance_brl_constant), 
       data=
         balance_deflated_r%>%
         group_by(date)%>%
         arrange(desc(direction))%>%
         summarise(balance_brl_constant=fob_brl_constant_bi[2]-fob_brl_constant_bi[1])%>%
         na.omit()
                   )+
  #scale_color_manual(values=c("blue", "red")) +
  geom_line()  +
  labs(color="", x="", y="R$ Bi (FOB) Deflated by CPI "%>%paste0(format(max(selected_deflated_r$date), "%m/%Y")), caption = "* 12 month rolling sums") +
  theme_linedraw() + 
  theme(legend.position="bottom") #+ scale_color_manual(values=c("red",  "blue")) 
```

![](README_files/figure-gfm/unnamed-chunk-4-1.png)<!-- -->

## By HS Code (6 digits)

``` r
by_dig <- comexstat_ncm() |> 
  filter(year>=2022) |>
  group_by(hs_dig=substr(ncm,1,6), direction, year)|>
  summarise(fob_usd=sum(fob_usd))|>
  collect()|>
  tidyr::pivot_wider(names_from=c("direction", "year"), values_from = fob_usd)%>%
  mutate(ep=exp_2023-exp_2022-1, ip=imp_2023-imp_2022-1, si=imp_2022+imp_2023)%>%
  arrange(desc(si))%>%
  head(30)
```

``` r
ggplot(aes(y=hs_dig, x=ip), data=by_dig) + geom_col()
```

    ## Don't know how to automatically pick scale for object of type <integer64>.
    ## Defaulting to continuous.

    ## Warning: Removed 30 rows containing missing values (`position_stack()`).

![](README_files/figure-gfm/unnamed-chunk-6-1.png)<!-- -->
