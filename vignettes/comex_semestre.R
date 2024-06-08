library(comexstatr)
#comexstat_download()

tmp <- comexstat_deflated(comexstat()%>%group_by(co_ano_mes, semestre=co_mes%in%1:6, fluxo)%>%summarise(vl_fob=sum(vl_fob), vl_cif=sum(vl_cif)))%>%collect()

tmps <- tmp%>%
  group_by(semestre, ano=lubridate::year(co_ano_mes))%>%summarise(across(starts_with("vl_"), function(x) (sum(x[fluxo=="exp"])-sum(x[fluxo=="imp"]))/1e9))


ggplot(aes(ano, vl_fob_current_brl), data=tmps%>%filter(semestre)) + geom_line()  + labs(x="", y="Balança Comercial do Brasil - Bilhões R$*", caption = paste("* Bilhões de R$ deflacionados pelo IPCA até", format(tmp$ipca_basedate[1], "%b/%Y")))  + geom_hline(yintercept=0, color="gray20", linetype=2, alpha=.25) + theme_classic()
