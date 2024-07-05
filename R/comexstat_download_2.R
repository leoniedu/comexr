#' Download Comexstat Data from MDIC (Brazilian Ministry of Development, Industry, Commerce, and Services)
#'
#' This function downloads Comexstat (Brazilian trade statistics) data from the
#' MDIC website for specified years, directions (imports/exports), and
#' types (NCM/HS4). It can also optionally download and manage auxiliary data tables.
#'
#' @param years A numeric vector or integer specifying the years for which data should be downloaded. Defaults to the current year (`2024`).
#' @param directions A character vector specifying the directions of trade: "imp" (imports) and/or "exp" (exports). Defaults to both.
#' @param types A character vector specifying the types of data: "ncm" (Nomenclatura Comum do Mercosul) and/or "hs4" (Harmonized System 4-digit). Defaults to both.
#' @param download_aux A logical value indicating whether to download auxiliary data tables (e.g., URF, VIA, country codes). Defaults to `TRUE`.
#' @param clean_aux A logical value indicating whether to delete existing auxiliary data files before downloading new ones. Defaults to `TRUE`.
#' @param cache A logical value indicating whether to use cached files if they exist. Defaults to `TRUE`.
#' @param .progress A logical value indicating whether to display a progress bar during downloads. Defaults to `TRUE`.
#' @param n_tries The maximum number of download attempts before giving up. Defaults to 30.
#' @param check A logical value indicating whether to perform data validation checks after downloading. Defaults to `FALSE`.
#' @param ... Additional arguments to be passed to `curl::multi_download`, such as `headers`, `handle`, etc.
#'
#' @details
#' This function constructs the URLs for the specified Comexstat data files based on the `years`, `directions`, and `types` arguments.
#' It then creates the necessary directories to store the data.
#'
#' Downloads are performed using `curl::multi_download`, with retry logic if there are initial failures. If `download_aux` is `TRUE`,
#' additional auxiliary data tables are downloaded as well.
#'
#' If `check` is `TRUE`, a basic data validation check is performed using the internal `comexstat_check` function.
#'
#' @return No direct return value. The function downloads the requested files to the specified directories.
#'
#' @examples
#' \dontrun{
#' # Download all data for 2023 and 2024
#' comexstat_download(years = 2023:2024)
#'
#' # Download only import data (NCM) for 2024
#' comexstat_download(years = 2024, directions = "imp", types = "ncm")
#' }
#'
#' @export
comexstat_download <- function(years = 2024, directions = c("imp", "exp"), types = c("hs4", "ncm"), download_aux = TRUE, clean_aux = TRUE, cache = TRUE, .progress = TRUE, n_tries = 30, check = FALSE, ...) {
  # Create necessary directories for storing the data (if they don't exist)
  sapply(file.path(comexstatr:::cdircomex, directions), dir.create, showWarnings = FALSE, recursive = TRUE)

  # Ensure types are valid (only "hs4" or "ncm")
  stopifnot(all(types %in% c("hs4", "ncm")))

  # Generate a table of all combinations of year, direction, and type to download
  todownload <- tidyr::crossing(
    tibble::tibble(year = years),
    tibble::tibble(direction = directions),
    tibble::tibble(type = types)
  ) |>
    # Create directory paths and file paths based on the year, direction, and type
    dplyr::mutate(
      dir = file.path(comexstatr:::cdircomex, type, paste0("direction=", direction)),
      path = file.path(dir,
                       dplyr::if_else(type == "hs4",
                                      paste0(tolower(direction), "_", year, "_mun.csv"),
                                      paste0(tolower(direction), "_", year, ".csv"))),
      # Generate the corresponding download URLs for each file
      url = dplyr::if_else(type == "hs4",
                           glue::glue("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/{toupper(direction)}_{year}_MUN.csv"),
                           glue::glue("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/{toupper(direction)}_{year}.csv"))
    )

  # Create any missing directories listed in `todownload`
  purrr::walk(todownload$dir |> unique(), dir.create, showWarnings = FALSE, recursive = TRUE)

  # Download auxiliary data tables if requested
  if (download_aux) {
    aux_data <- tibble::tibble(url = c(
      # ... list of URLs for auxiliary tables
    )) |>
      # Define the local path for each auxiliary table
      dplyr::mutate(path = file.path(comexstatr:::cdircomex, basename(url) |> tolower()))

    # Delete existing auxiliary files if clean_aux is TRUE
    if (clean_aux) unlink(aux_data$path)

    # Combine the main download table with the auxiliary table
    todownload <- todownload |> dplyr::bind_rows(aux_data)
  }

  # Add a column to track download status
  todownload |> dplyr::mutate(ok = FALSE) -> todownload

  # Download files with retry mechanism
  j <- 1
  while ((j <= n_tries) & any(!todownload$ok)) {
    if (j > 1) print(glue::glue("Try #{j}!"))
    res <- curl::multi_download(todownload$url,
                                destfiles = todownload$path, progress = .progress,
                                resume = cache, timeout = 100, multiplex = TRUE, ...)
    todownload$ok <- (res$success %in% TRUE)
    j <- j + 1
  }

  # Delete files with URLs that weren't found
  todelete <- res |>
    dplyr::filter(!grepl("balanca.economia.gov.br", url)) |>
    dplyr::pull(destfile)
  unlink(todelete)
  if (length(todelete) > 0) warning(glue::glue("{length(todelete)} urls not found."))

  # Stop if there are any failed downloads after all retries
  if (any(!todownload$ok)) stop("Error downloading data.")

  # Run optional data validation check
  if (check) comexstatr:::comexstat_check()
}
