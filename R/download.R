get_cpi <- function() {
    path <- "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL"
    cpi_0 <- read.csv(path, header = FALSE, col.names = c("date", "cpi"), skip = 1)
    transform(cpi_0, date = as.Date(date)) |>
        dplyr::filter(date >= as.Date("1997-01-01"))
}

get_ipca <- function() {
    ipca0 <- rbcb::get_series(433, start_date = "1997-01-01")
    names(ipca0)[2] <- "ipca"
    ipca <- transform(ipca0[order(ipca0$date), ], ipca_i = cumprod(1 + ipca/100))
    ipca
}

get_brlusd <- function(from = "1997-01-01", to = NULL) {
    if (is.null(to)) {
        lastmonth <- {
            m <- Sys.Date()
            lubridate::day(m) <- 1
            m - 1
        }
    } else lastmonth <- to
    brlusd0 <- rbcb::get_currency("USD", from, lastmonth)
    brlusd <- brlusd0 |>
        dplyr::group_by(date = lubridate::make_date(lubridate::year(date), lubridate::month(date))) |>
        dplyr::summarise(brlusd = (mean(bid) + mean(ask))/2)
    brlusd
}


#' Download and processes deflators (CPI/USA, IPCA/Brazil, Exchange rate BRL/USE)
#'
#' @param updated
#'
#' @return A data frame with columns co_ano_mes (date), ipca (monthly inflation from Brazil), ipca_i (monthly inflation from Brazil indexed such as 1997-01-01 is 1), cpi index (monthly inflation from USA).
#' @export
#'
get_deflators <- function(updated = Sys.Date(), na_omit = FALSE) {
    res <- get_ipca() |>
        dplyr::full_join(get_brlusd(), by = "date") |>
        dplyr::full_join(get_cpi(), by = "date") |>
        dplyr::arrange(date)
    if (na_omit)
        res <- na.omit(res)
    res
}



