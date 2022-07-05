.onAttach <- function(...) {
  msg(cli::rule(crayon::bold("Comexstatr")))
  msg(" ")
  msg("La documentacion del paquete y ejemplos ...")
  msg("Esta libreria necesita ... GB libres para la crear la base de datos localmente. Una vez creada la base, esta ocupa ... GB en disco.")
  msg(" ")
  if (interactive() && Sys.getenv("RSTUDIO") == "1"  && !in_chk()) {
    comexstat_pane()
  }
  if (interactive()) comexstat_status()
}
