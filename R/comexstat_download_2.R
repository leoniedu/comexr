comex_rewrite <- function(data, path=NULL, ...) {
    if("ncm"%in%names(data)) type_ <- "ncm" else type_ <- 'hs4'
    dpath <- path %||% file.path(ddircomex,type_)
    dir.create(dpath, recursive = TRUE, showWarnings = FALSE)
    if (type_=="ncm") {
        data|>
            dplyr::group_by(year, direction)|>
            dplyr::arrange(ncm,month,country_code)|>
            arrow::write_dataset(path = dpath, ...)
    } else if (type_=="hs4") {
        data|>
            dplyr::group_by(year, direction)|>
            dplyr::arrange(hs4,month,country_code)|>
            arrow::write_dataset(path = dpath, ...)
    }
    years_ <- data|>dplyr::ungroup()|>dplyr::distinct(year)|>dplyr::collect()|>dplyr::pull(year)
    directions_ <- data|>dplyr::ungroup()|>dplyr::distinct(direction)|>dplyr::collect()|>dplyr::pull(direction)
    comex_check(years=years_, directions = directions_, type = type_)
}


#' Download Comexstat Data from MDIC (Brazilian Ministry of Development, Industry, Commerce, and Services)
#'
#' This function downloads Comexstat (Brazilian trade statistics) data from the MDIC website for specified years,
#' directions (imports/exports), and types (NCM/HS4). It can also optionally download and manage auxiliary data tables.
#' After download and checking the csv files, the data is stores as parquet files under
#' a data directory, in order to increase speed.
#' @param years A numeric vector or integer specifying the years for which data should be downloaded.
#'   Defaults to the current year.
#' @param directions A character vector specifying the directions of trade: 'imp' (imports) and/or 'exp' (exports).
#'   Defaults to both.
#' @param types A character vector specifying the types of data: 'ncm' (Nomenclatura Comum do Mercosul)
#'   and/or 'hs4' (Harmonized System 4-digit). Defaults to both.
#' @param force_download_aux A logical value indicating whether to force the download of auxiliary data tables
#'   (e.g., URF, VIA, country codes), even if they already exist in the cache. Defaults to `FALSE`.
#'   Auxiliary data is typically downloaded when a new trade data file is downloaded.
#' @param cache A logical value indicating whether to use cached files if they exist. Defaults to `TRUE`.
#' @param .progress A logical value indicating whether to display a progress bar during downloads.
#'   Defaults to `TRUE`.
#' @param n_tries The maximum number of download attempts before giving up. Defaults to 30.
#' @param timeout The maximum time (in seconds) to wait for a download response. Defaults to 600 seconds (10 minutes).
#' @param ... Additional arguments to be passed to `curl::multi_download`, such as `headers`, `handle`, etc.
#'
#' @details
#' This function performs the following steps:
#'
#' 1. **File Structure:** Ensures the necessary directories exist to store downloaded files.
#' 2. **URL Generation:** Constructs URLs for Comexstat data files based on the specified years, directions, and types.
#' 3. **Download (with Retry):** Downloads files using `curl::multi_download` with retry logic in case of failures.
#' 4. **Auxiliary Data:** If there is new trade data or `force_download_aux` is `TRUE`, it downloads and manages auxiliary data tables.
#' 5. **Error Handling:** Checks if any downloads failed or if the downloaded files are valid.
#' 6. **Write parquet files:** Stores data as parquet files in order to speed up analyses.

#'
#' @return `invisible(NULL)` if successful. The function primarily downloads data to the specified directories.
#'
#' @examples
#' \dontrun{
#' # Download all data for 2023 and 2024
#' comex_download(years = 2023:2024)
#'
#' # Download only import data (NCM) for 2024
#' comex_download(years = 2024, directions = 'imp', types = 'ncm')
#' }
#'
#' @export
comex_download <- function(years = 2024, directions = c("imp", "exp"), types = c("hs4", "ncm"), cache = TRUE, .progress = TRUE,
    n_tries = 30, force_download_aux = FALSE, timeout = 600, ...) {
    # Ensure types are valid (only 'hs4' or 'ncm')
    stopifnot(all(types %in% c("hs4", "ncm")))
    # Generate a table of all combinations of year, direction, and type to download
    todownload <- tidyr::crossing(tibble::tibble(year = years), tibble::tibble(direction = directions), tibble::tibble(type = types)) |>
        # Create directory paths and file paths based on the year, direction, and type
    dplyr::mutate(dir = file.path(comexr:::cdircomex, type, paste0("direction=", direction)), path = file.path(dir,
        dplyr::if_else(type == "hs4", paste0(tolower(direction), "_", year, "_mun.csv"), paste0(tolower(direction),
            "_", year, ".csv"))), url = dplyr::if_else(type == "hs4", glue::glue("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/{toupper(direction)}_{year}_MUN.csv"),
        glue::glue("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/{toupper(direction)}_{year}.csv")))

    # Create any missing directories listed in `todownload`
    purrr::walk(todownload$dir |>
        unique(), dir.create, showWarnings = FALSE, recursive = TRUE)

    # Add a column to track download status
    todownload |>
        dplyr::mutate(ok = FALSE) -> todownload

    # Download files with retry mechanism
    j <- 1
    while ((j <= n_tries) && any(!todownload$ok)) {
        if (j > 1)
            print(glue::glue("Try #{j}!"))
        res <- curl::multi_download(todownload$url, destfiles = todownload$path, progress = .progress, resume = cache,
            timeout = timeout, multiplex = TRUE, accept_encoding = c("gzip,deflate"), ...)
        todownload$ok <- (res$success %in% TRUE)
        j <- j + 1
    }
    # Delete files with URLs that weren't found
    todelete_0 <- res |>
        dplyr::filter(!grepl("balanca.economia.gov.br", url))
    todelete <- todownload |>
        dplyr::semi_join(todelete_0, by = c(path = "destfile"))
    unlink(todelete$path)
    if (nrow(todelete) > 0)
        warning(glue::glue("URLs: \n{paste(todelete$url, collapse='\n')}\nnot found."))
    ## update todownload to remove not downloaded
    todownload <- todownload |>
        dplyr::anti_join(todelete, by = dplyr::join_by(year, direction, type, dir, path, url, ok))
    if (any(!todownload$ok))
        stop("Failed to download all files.")

    ## check downloads
    problem_down <- sapply(todownload$path, function(z) "try-error" %in% try(arrow::open_delim_dataset(sources = z,
        delim = ";") |>
        dplyr::count(CO_ANO) |>
        dplyr::collect()))
    if (any(problem_down)) {
        unlink(names(problem_down[problem_down]))
        stop("Problem downloading data. Partial data deleted. You can try again.")
    }
    ## write as parquet
    ncm <- todownload|>dplyr::filter(type=="ncm")
    hs4 <- todownload|>dplyr::filter(type=="hs4")
    any_downloaded <- any(res$status_code %in% c(200, 206))
    if (any_downloaded && (nrow(ncm)>0)) {
        comex_rewrite(comex_ncm_raw()|>filter(year%in%years, direction%in%directions), existing_data_behavior='delete_matching')
    }
    if (any_downloaded && (nrow(hs4)>0)) {
        comex_rewrite(comex_hs4_raw()|>filter(year%in%years, direction%in%directions), existing_data_behavior='delete_matching')
    }
    # Download auxiliary data tables if any new file downloaded
    if (any_downloaded | force_download_aux) {
        aux_data <- tibble::tibble(url = c(
            "https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv",
            "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_TOTAIS_CONFERENCIA.csv",
            "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_TOTAIS_CONFERENCIA.csv",
            "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/EXP_TOTAIS_CONFERENCIA_MUN.csv",
            "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/IMP_TOTAIS_CONFERENCIA_MUN.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CUCI.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_ISIC.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CGCE.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv",
            "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_UNIDADE.csv")) |>
            dplyr::mutate(path = file.path(comexr:::cdircomex, basename(url) |>
                tolower()), ok = FALSE)
        # Delete existing auxiliary files
        unlink(aux_data$path)
        j <- 1
        while ((j <= n_tries) && any(!aux_data$ok)) {
            if (j > 1)
                print(glue::glue("Try #{j}!"))
            res <- curl::multi_download(aux_data$url, destfiles = aux_data$path, progress = .progress, resume = cache,
                timeout = timeout, multiplex = TRUE, ...)
            aux_data$ok <- (res$success %in% TRUE)
            j <- j + 1
        }
        # Stop if there are any failed downloads after all retries
        if (any(!todownload$ok))
            stop("Error downloading aux data.")
    }
}
