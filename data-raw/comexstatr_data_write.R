#library(comexstatr)
library(dplyr)
library(arrow)
## create directory for data package
#cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
con <- comexstatr::comexstat_connect()
# comexstat <- tbl(con, "comexstatv")%>%
#   #filter(co_ano=="2022")%>%
#   mutate(co_ano=substr(co_ano_mes,1,4))%>%
#   to_arrow()
# # dout <- 'data-raw/comexstat_feather'
# # dout <- 'data-raw/comexstat_parquet'
# dout <- "data-raw/ano"
# unlink(dout, recursive = TRUE)
# dir.create(dout, recursive = TRUE)
# comexstat %>%
#   group_by(co_ano, fluxo) %>%
#   write_dataset(dout, format = "parquet")

ncms <- tbl(con,  "ncms")%>%collect
usethis::use_data(ncms, overwrite = TRUE, ascii=TRUE)
pais <- tbl(con,  "pais")%>%collect
usethis::use_data(pais, overwrite = TRUE, ascii=TRUE)
pais_bloco <- tbl(con,  "pais_bloco")%>%collect
usethis::use_data(pais_bloco, overwrite = TRUE, ascii=TRUE)


## por pais e ano
dout <- "data-raw/pais"

unlink(dout, recursive = TRUE)

dir.create(dout, recursive = TRUE)
comexstatp <- tbl(con, "comexstatvp")%>%
  mutate(co_ano=substr(co_ano_mes,1,4))%>%
  to_arrow()
comexstatp %>%
  group_by(co_ano, fluxo) %>%
  write_dataset(dout, format = "parquet")


lanomes <- tbl(con, "comexstatvp")%>%
  summarise(co_ano_mes=max(co_ano_mes))%>%
  pull(co_ano_mes)

comexstat_last <-
  open_dataset(
  dout,
  format = "parquet"
)%>%
  filter(co_ano_mes==lanomes)%>%
  collect
usethis::use_data(comexstat_last, overwrite=TRUE)



comexstat_mes <- tbl(con, "comexstatv")%>%
  filter(lubridate::month(co_ano_mes)==lubridate::month(lanomes))%>%
  collect

usethis::use_data(comexstat_mes, overwrite=TRUE)


