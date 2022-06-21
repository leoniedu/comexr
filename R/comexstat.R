#' @export
comexstat_duck <- function(data_dir = rappdirs::user_data_dir("comexstatr"),
                           fname_duck = "comexstat.duckdb", overwrite = FALSE) {
  fname_duck <- file.path(data_dir, fname_duck)
  if (overwrite) unlink(fname_duck)
  drv <- duckdb::duckdb(dbdir = path.expand(fname_duck))
  duckdb::dbConnect(drv)
}
