sql_action <- function() {
  if (requireNamespace("rstudioapi", quietly = TRUE) &&
      exists("documentNew", asNamespace("rstudioapi"))) {
    contents <- paste(
      "-- !preview conn=comexstatr::comexstat_connect()",
      "",
      "SELECT * FROM ncms",
      "",
      sep = "\n"
    )

    rstudioapi::documentNew(
      text = contents, type = "sql",
      position = rstudioapi::document_position(2, 40),
      execute = FALSE
    )
  }
}

comexstat_pane <- function() {
  observer <- getOption("connectionObserver")
  if (!is.null(observer) && interactive()) {
    observer$connectionOpened(
      type = "Comexstat",
      host = "comexstat",
      displayName = "Comexstat tables",
      icon = system.file("img", "cl-logo.png", package = "comexstat"),
      connectCode = "comexstat_pane()",
      disconnect = comexstat_disconnect,
      listObjectTypes = function() {
        list(
          table = list(contains = "data")
        )
      },
      listObjects = function(type = "datasets") {
        DBI::dbGetQuery(comexstat_connect(), "select table_name as name, 'table'
 as type from information_schema.tables where table_type!='VIEW'")
      },
      listColumns = function(table) {
        res <- DBI::dbGetQuery(comexstat_connect(),
                               paste("SELECT * FROM", table, "LIMIT 1"))
        data.frame(
          name = names(res), type = vapply(res, function(x) class(x)[1],
                                           character(1)),
          stringsAsFactors = FALSE
        )
      },
      previewObject = function(rowLimit, table) {
        DBI::dbGetQuery(comexstat_connect(),
                        paste("SELECT * FROM", table, "LIMIT", rowLimit))
      },
      actions = list(
        Status = list(
          icon = system.file("img", "ropensci-logo.png", package = "comexstat"),
          callback = comexstat_status
        ),
        SQL = list(
          icon = system.file("img", "edit-sql.png", package = "comexstat"),
          callback = sql_action
        )
      ),
      connectionObject = comexstat_connect()
    )
  }
}

update_comexstat_pane <- function() {
  observer <- getOption("connectionObserver")
  if (!is.null(observer)) {
    observer$connectionUpdated("Comexstat", "comexstatr", "")
  }
}
