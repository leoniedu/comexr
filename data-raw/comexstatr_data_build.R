library(comexstatr)
library(tictoc)
comexstat_download_raw()

ymin <- 0
yminc <- lubridate::year(Sys.Date())-2
comexstat_stage(year_min_ncm=ymin, year_min_ncm_country=yminc)



comexstat_create_db(overwrite=TRUE)
tic()
comexstat_process(year_min_ncm=ymin, year_min_ncm_country=yminc, threads = 2, mem_limit_gb = 8)
toc()

stop()


tmp <- map(dbListTables(con), ~ try(tbl(con, .x)%>%head%>%collect))
names(tmp) <- dbListTables(con)

## export comexstatv


library(comexstatr)
library(tictoc)
library(dplyr)
library(arrow)
tic()
con <- comexstat_connect()
ddir_partition <- file.path(comexstatr:::comexstat_path(), "comexstat12")
unlink(ddir_partition, recursive = TRUE)
dir.create(ddir_partition, showWarnings = FALSE)
df <- tbl(con, "comexstat12")%>%collect
toc()
tic()
df %>%
  group_by(co_ano, fluxo) %>%
  write_dataset(ddir_partition, format = "parquet")
toc()
