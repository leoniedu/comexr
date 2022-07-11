library(comexstatr)
library(tictoc)
comexstat_download_raw()

ymin <- 2020
yminc <- lubridate::year(Sys.Date())-2
comexstat_stage(year_min_ncm=ymin, year_min_ncm_country=yminc)



comexstat_create_db(overwrite=TRUE)
tic()
comexstat_process(year_min_ncm=ymin, year_min_ncm_country=yminc, threads = 8, mem_limit_gb = 8)
toc()


con <- comexstat_connect()
