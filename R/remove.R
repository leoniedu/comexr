#' Elimina la Base de Datos de tu Computador
#'
#' Elimina el directorio `comexstat` y todos sus contenidos, incluyendo versiones
#' de la base de datos del comexstat creadas con cualquier version de 'DuckDB'.
#'
#' @param preguntar Si acaso se despliega un menu para confirmar la accion de
#'  borrar cualquier base del comexstat existente. Por defecto es verdadero.
#' @return NULL
#' @export
#'
#' @examples
#' \dontrun{ comexstat_eliminar() }
comexstat_delete <- function(ask = TRUE) {
  if (ask) {
    answer <- utils::menu(c("Ok", "No!"),
                   title = "Delete all comexstat files",
                   graphics = FALSE)
    if (answer == 2) {
       return(invisible())
    }
  }
  suppressWarnings(comexstat_desconectar())
  try(unlink(comexstat_path(), recursive = TRUE))
  update_comexstat_pane()
  return(invisible())
}
