comexstat_path <- function() {
  sys_comexstat_path <- Sys.getenv("COMEXSTAT_DIR")
  sys_comexstat_path <- gsub("\\\\", "/", sys_comexstat_path)
  if (sys_comexstat_path == "") {
    return(gsub("\\\\", "/", tools::R_user_dir("comexstatr")))
  } else {
    return(gsub("\\\\", "/", sys_comexstat_path))
  }
}

comexstat_check_status <- function() {
  if (!comexstat_status(FALSE)) {
    stop("Local database for Comexstat is empty. Download it with comexstat_download().")
  }
}

#' Comexstat connection
#'
#' Devuelve una conexion a la base de datos local.
#'
#' @param dir Path to local database. By default
#' `comexstat` local R data path  or the system variable
#' `COMEXSTAT_DIR` if it exists.
#'
#' @export
#'
#' @examples
#' \dontrun{
#'  DBI::dbListTables(comexstat_connect())
#'
#' }
comexstat_connect <- function(dir = comexstat_path(), overwrite = FALSE) {
  duckdb_version <- utils::packageVersion("duckdb")
  db_file <- paste0(dir, "/comexstat_duckdb_v", gsub("\\.", "", duckdb_version), ".sql")
  db <- mget("comexstat_connect", envir = comexstat_cache, ifnotfound = NA)[[1]]
  if (inherits(db, "DBIConnection")) {
    if (overwrite) {
      DBI::dbDisconnect(db, shutdown=TRUE)
    } else if (DBI::dbIsValid(db)) {
      return(db)
    }
  }
  try(dir.create(dir, showWarnings = FALSE, recursive = TRUE))
  if (overwrite) unlink(db_file)
  drv <- duckdb::duckdb(db_file, read_only = FALSE)
  tryCatch({
    con <- DBI::dbConnect(drv)
  },
  error = function(e) {
    if (grepl("Failed to open database", e)) {
      stop(
        "Local database is open in another R session. Please disconnect using comexstat_disconnet() in the remaining ressions.",
        call. = FALSE
      )
    } else {
      stop(e)
    }
  },
  finally = NULL
  )
  comexstat_arrow(con)
  assign("comexstat_connect", con, envir = comexstat_cache)
  con
}

#' Tablas Completas de la Base de Datos
#'
#' Devuelve una tabla completa de la base de datos. Para entregar datos
#' filtrados previamente se debe usar [comexstatr::comexstat_connect()].
#'
#' @param tabla Una cadena de texto indicando la tabla a extraer
#' @return Un tibble
#' @export
#'
#' @examples
#' \dontrun{ comexstat_tabla("comunas") }
comexstat_table <- function(table) {
  df <- tryCatch(
    tibble::as_tibble(DBI::dbReadTable(comexstat_connect(), table)),
    error = function(e) { read_table_error(e) }
  )
  return(df)
}

#' Desconecta la Base de Datos
#'
#' Una funcion auxiliar para desconectarse de la base de datos.
#'
#' @examples
#' comexstat_disconnect()
#' @export
#'
comexstat_disconnect <- function(environment = comexstat_cache) {
  db <- mget("comexstat_connect", envir = comexstat_cache, ifnotfound = NA)[[1]]
  if (inherits(db, "DBIConnection")) {
    DBI::dbDisconnect(db, shutdown = TRUE)
  }
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    observer$connectionClosed("ComexstatR", "comexstat")
  }
}

comexstat_status <- function(msg = TRUE) {
  expected_tables <- sort(comexstat_tables())
  existing_tables <- sort(DBI::dbListTables(comexstat_connect()))
  if (isTRUE(all.equal(expected_tables, existing_tables))) {
    status_msg <- crayon::green(paste(cli::symbol$tick,
    "La base de datos local esta OK."))
    out <- TRUE
  } else {
    status_msg <- crayon::red(paste(cli::symbol$cross,
    "La base de datos local esta vacia, daniada o no es compatible con tu version de duckdb. Descargala con comexstat_download()."))
    out <- FALSE
  }
  if (msg) msg(status_msg)
  invisible(out)
}

comexstat_tables <- function() {
  c("ncm")
}

comexstat_cache <- new.env()
reg.finalizer(comexstat_cache, comexstat_disconnect, onexit = TRUE)
