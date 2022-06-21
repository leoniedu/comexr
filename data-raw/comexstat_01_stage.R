library(arrow)
library(rappdirs)
library(dplyr)
library(tictoc)
library(pins)
comexstat_board <- board_local(versioned = FALSE)

tic()


## In the current release, arrow supports the dplyr verbs mutate(), transmute(), select(), rename(), relocate(), filter(), and arrange().

comexstat_schema_e <- schema(
  field("CO_ANO", int16()),
  field("CO_MES", int8()),
  field("CO_NCM", string()),
  field("CO_UNID", string()),
  field("CO_PAIS", string()),
  field("SG_UF_NCM", string()),
  field("CO_VIA", string()),
  field("CO_URF", string()),
  field("QT_ESTAT", double()),
  field("KG_LIQUIDO", double()),
  field("VL_FOB", double())
)

comexstat_schema_i <- comexstat_schema_e
comexstat_schema_i <- comexstat_schema_i$AddField(11, field = field("VL_FRETE", double()))
comexstat_schema_i <- comexstat_schema_i$AddField(12, field = field("VL_SEGURO", double()))


## export
fname <- file.path(cdir, "EXP_COMPLETA.csv")
cnames <- read.csv2(fname, nrows = 3) %>%
  janitor::clean_names() %>%
  names()
df_e <- open_dataset(
  fname,
  delim = ";",
  format = "text",
  schema = comexstat_schema_e,
  read_options = CsvReadOptions$create(skip_rows = 1, column_names = toupper(cnames))
)


fname <- file.path(cdir, "IMP_COMPLETA.csv")
cnames <- read.csv2(fname, nrows = 3) %>%
  janitor::clean_names() %>%
  names()
df_i <- open_dataset(
  fname,
  delim = ";",
  format = "text",
  schema = comexstat_schema_i,
  read_options = CsvReadOptions$create(skip_rows = 1, column_names = toupper(cnames))
)

## bind together imports and exports
df <- open_dataset(list(df_i, df_e)) %>%
  rename_with(tolower) %>%
  mutate(fluxo = if_else(is.na(vl_frete), "exp", "imp"))

## write partitioned data
ddir_partition <- file.path(ddir, "comexstat_partition")
unlink(ddir_partition, recursive = TRUE)
dir.create(ddir_partition, showWarnings = FALSE)
df %>%
  group_by(co_ano, fluxo) %>%
  write_dataset(ddir_partition, format = "feather")
toc()

tic()
## partition by pais
ddir_partition <- file.path(ddir, "comexstat_pais")
unlink(ddir_partition, recursive = TRUE)
dir.create(ddir_partition, showWarnings = FALSE)
df %>%
  filter(co_ano >= yminp) %>%
  group_by(co_pais, fluxo) %>%
  write_dataset(ddir_partition, format = "feather")
toc()
