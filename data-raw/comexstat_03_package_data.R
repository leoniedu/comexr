library(duckdb)
library(dplyr)

comexstat_duck_1 <- function(data_dir = rappdirs::user_data_dir("comexstatr"),
                           fname_duck = "comexstat.duckdb", overwrite = FALSE) {
  fname_duck <- file.path(data_dir, fname_duck)
  if (overwrite) unlink(fname_duck)
  drv <- duckdb::duckdb(dbdir = path.expand(fname_duck))
  duckdb::dbConnect(drv)
}
con_comex <- comexstat_duck_1()

##ncms
ncms <- tbl(con_comex, "ncms") %>% collect
usethis::use_data(ncms, overwrite = TRUE, compress = TRUE)

## trade data (with rolling sum) in wide format
comex_roll12_wide <- tbl(con_comex, "comexstat12") %>%
  group_by(co_ncm, co_ano_mes, fluxo) %>%
  summarise(vl_fob_12_sum = sum(vl_fob_12_sum, na.rm = TRUE)) %>%
  select(co_ncm, co_ano_mes, vl_fob_12_sum, fluxo) %>%
  collect() %>%
  tidyr::pivot_wider(names_from = fluxo, values_from = vl_fob_12_sum, values_fill = 0) %>%
  arrange(co_ano_mes, co_ncm)%>%
  ungroup()

usethis::use_data(comex_roll12_wide, overwrite = TRUE, compress = TRUE)

comex_roll12 <- tbl(con_comex, "comexstat12")%>%collect
usethis::use_data(comex_roll12, overwrite = TRUE, compress = TRUE)

comex_roll12p <- tbl(con_comex, "comexstat12p")%>%collect
usethis::use_data(comex_roll12p, overwrite = TRUE, compress = TRUE)


dbDisconnect(con_comex, shutdown = TRUE)

