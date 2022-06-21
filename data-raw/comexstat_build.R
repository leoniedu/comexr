library(rappdirs)
library(duckdb)
library(here)

comexstat_duck_1 <- function(data_dir = rappdirs::user_data_dir("comexstatr"),
                             fname_duck = "comexstat.duckdb", overwrite = FALSE) {
  fname_duck <- file.path(data_dir, fname_duck)
  if (overwrite) unlink(fname_duck)
  drv <- duckdb::duckdb(dbdir = path.expand(fname_duck))
  duckdb::dbConnect(drv)
}


## echo scripts?
echonow <- TRUE

## force downloading data, even if unchanged
force_download <- FALSE

## minimum year for the rolling sum by year (ano), month (mes), and product (ncm)
ymin <- 2017

## minimum year for the rolling sum by country (pais), year (ano), month (mes), and product (ncm),
yminp <- 2019

ddir <- path.expand(rappdirs::user_data_dir("comexstatr"))
cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))


source(here("data-raw/comexstat_00_download.R"), echo=echonow)
source(here("data-raw/comexstat_01_stage.R"), echo=echonow)
source(here("data-raw/comexstat_02_process.R"), echo=echonow)
source(here("data-raw/comexstat_03_package_data.R"), echo=echonow)

## build package
detach(package:camexstat)
# rm(list=ls())
devtools::document(roclets = c("rd", "collate", "namespace"))
system("R CMD INSTALL --no-multiarch --with-keep.source ../comexstatr")
toc()
