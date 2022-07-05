library(comexstatr)
library(tictoc)
comexstat_download_raw()
comexstat_stage(year_min_ncm=2018, year_min_ncm_country=2018)



comexstat_create_db(overwrite=TRUE)
tic()
comexstat_process(year_min_ncm=2018, year_min_ncm_country=2019#, threads = 1, mem_limit_gb = 8
                  )
toc()


# > comexstat_process(year_min_ncm=0, year_min_ncm_country=2019, threads = 1, mem_limit_gb = 6)
# Creating ncm/country/month level dataset  ...
# 284.862 sec elapsed
# Creating ncm/month level dataset  ...
# 124.248 sec elapsed
# > toc()
# 409.479 sec elapsed

  # >   comexstat_process(year_min_ncm=2017, year_min_ncm_country=2019, threads = 2, mem_limit_gb = 2)
  # Creating ncm/country/month level dataset  ...
  # Error: rapi_execute: Failed to run query
  # Error: Out of Memory Error: could not allocate block of 262144 bytes
  #
#
#   >   comexstat_process(year_min_ncm=2017, year_min_ncm_country=2019, threads = 2, mem_limit_gb = 4)
#   Creating ncm/country/month level dataset  ...
#   240.612 sec elapsed
#   Creating ncm/month level dataset  ...
#   15.141 sec elapsed
#   >   toc()
#   255.809 sec elapsed

  # comexstat_process(year_min_ncm=0, year_min_ncm_country=2019, threads = 2, mem_limit_gb = 4)
  # Creating ncm/country/month level dataset  ...
  # 225.031 sec elapsed
  # Creating ncm/month level dataset  ...
  # Error: rapi_execute: Failed to run query
  # Error: Out of Memory Error: could not allocate block of 2097160 bytes


# comexstat_process(year_min_ncm=2017, year_min_ncm_country=2019, tv="table")
# Creating ncm/country/month level dataset  ...
# 339.811 sec elapsed
# Creating ncm/month level dataset  ...
# 68.96 sec elapsed

# > comexstat_process(year_min_ncm=2017, year_min_ncm_country=2019, tv="view")
# Creating ncm/country/month level dataset  ...
# 22.668 sec elapsed
# Creating ncm/month level dataset  ...
# 37.229 sec elapsed
# > toc()
# 60.737 sec elapsed

##tv table, 2019, 2019: 47+37=85 secs
##tv view, 2019, 2019: 9+25=36 secs
#con <- comexstat_connect()

tbl(comexstat_connect(), "comexstat")
