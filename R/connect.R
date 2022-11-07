#' @export
comexstat_path <- function() {
  sys_comexstat_path <- Sys.getenv("COMEXSTAT_DIR")
  sys_comexstat_path <- gsub("\\\\", "/", sys_comexstat_path)
  if (sys_comexstat_path == "") {
    return(gsub("\\\\", "/", tools::R_user_dir("comexstatr")))
  } else {
    return(gsub("\\\\", "/", sys_comexstat_path))
  }
}
