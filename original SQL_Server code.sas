-- SVUOTO LA TABELLA mari_aggiudicatari E LA RIEMPIO CON I NUOVI VALORI
DELETE from mari_aggiudicatari

select
	gpu_cod_gara,
	gpu_progr_ubi,
	gpu_cod_concorrente,
	gco_nome,
	sum(gpu_punteggio) 
from
	gar_gare_punteggi join gar_gare_struttura on (gst_cod_gara=gpu_cod_gara and gst_progr=gpu_progr) 
 			join gar_concorrenti_tot on (gpu_cod_concorrente=gco_cod_conco)
 	where gst_gruppo not in ('G3') and gst_criterio not in ('PR','SC')
 	group by gpu_cod_gara,gpu_progr_ubi,gpu_cod_concorrente,gco_nome
 	order by gpu_cod_gara,gpu_progr_ubi,sum(gpu_punteggio) desc


--estraggo tutto il mondo Dussmann
select distinct  gga_cod_gara,gga_scad_iscrizione,gcl_nome,gga_spec_cliente,gga_stato,gst_descr_stato,gga_gg_durata,gga_tm_durata,gga_tm_importo,
gga_tm_oggetto,gga_tm_categorie,dbo.fbi_concatena_categorie(gar_gare.gga_cod_gara),--gct_cod_telemat,gct_descr_telemat,gct_cod_dussmann,gct_descr_dussmann,
gub_progr,gub_descr_ubic,gub_cod_avanzamento,gsa_descr_stato_av,
gub_data_apertura_amministrativa,gub_data_apertura_economica,gub_tm_dt_aggiudicazione,
case when gub_importo=0 then gga_tm_importo else gub_importo end,--mari_aggiudicatari.gpu_cod_concorrente,gco_nome,
dbo.fbi_concatena_settori(gar_gare.gga_cod_gara),'',gga_settore_dussmann, gub_settore_dussmann,mari_aggiudicatari.gpu_cod_concorrente,mari_aggiudicatari.gco_nome,mari_aggiudicatari.punteggio_finale

from gar_gare
left join gar_stato on (gga_stato=gar_stato.gst_cod_stato)
left join gar_clienti on (gga_cliente=gcl_cod_cliente)
left join gar_gare_categorie on (gga_cod_gara=gct_cod_gara)
left join gar_gare_ubicazioni on (gga_cod_gara=gub_cod_gara)
left join gar_gare_settori on (gga_cod_gara=ggs_cod_gara)
left join gar_stato_av on (gub_cod_avanzamento=gar_stato_av.gsa_cod_stato_av)
left join mari_aggiudicatari on (mari_aggiudicatari.gpu_cod_gara=gga_cod_gara and mari_aggiudicatari.gpu_progr_ubi=gub_progr and 
							mari_aggiudicatari.punteggio_finale=(select max (appoggio.punteggio_finale) 
																		from mari_aggiudicatari appoggio
																		where mari_aggiudicatari.gpu_cod_gara=appoggio.gpu_cod_gara and mari_aggiudicatari.gpu_progr_ubi=appoggio.gpu_progr_ubi
																		)) 
 where exists (select 1 from gar_gare_struttura where gst_cod_gara=gga_cod_gara )
 --and gga_cod_gara=12348957
order by gga_cod_gara,gub_progr

--estraggo il resto del mondo


select distinct  gga_cod_gara,gga_scad_iscrizione,gcl_nome,gga_spec_cliente,gga_stato,gst_descr_stato,gga_gg_durata,gga_tm_durata,gga_tm_importo,
gga_tm_oggetto,gga_tm_categorie,dbo.fbi_concatena_categorie(gar_gare.gga_cod_gara),--gct_cod_telemat,gct_descr_telemat,gct_cod_dussmann,gct_descr_dussmann,
gub_progr,gub_descr_ubic,gub_cod_avanzamento,gsa_descr_stato_av,
gub_data_apertura_amministrativa,gub_data_apertura_economica,isnull(gub_tm_dt_aggiudicazione,gce_dt_aggiudicazione),
case when gub_importo=0 then gga_tm_importo else gub_importo end,--mari_aggiudicatari.gpu_cod_concorrente,gco_nome,
dbo.fbi_concatena_settori(gar_gare.gga_cod_gara),'',gga_settore_dussmann, gub_settore_dussmann,'',gce_vincitore_ragsoc,''

from gar_gare
left join gar_stato on (gga_stato=gar_stato.gst_cod_stato)
left join gar_clienti on (gga_cliente=gcl_cod_cliente)
left join gar_gare_categorie on (gga_cod_gara=gct_cod_gara)
left join gar_gare_ubicazioni on (gga_cod_gara=gub_cod_gara)
left join gar_gare_settori on (gga_cod_gara=ggs_cod_gara)
left join gar_stato_av on (gub_cod_avanzamento=gar_stato_av.gsa_cod_stato_av)
left join gar_csv_esiti on (gce_rifgara=gga_cod_gara)

 where not exists (select 1 from gar_gare_struttura where gst_cod_gara=gga_cod_gara )
 --and gga_cod_gara=12348957
order by gga_cod_gara,gub_progr