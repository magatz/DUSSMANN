/* libname aaa "/mnt/storage/sasdata/DUSSMANN/"; */
proc fedsql sessref=mySession;
 create table casuser.step_1  {options replace=true}  as
select
	A.gpu_cod_gara,
	A.gpu_progr_ubi,
	A.gpu_cod_concorrente,
	C.gco_nome,
	sum(A.gpu_punteggio) as sum_punteggi 
from
	dussmann.gar_gare_punteggi as A join dussmann.gar_gare_struttura as B on
		 (B.gst_cod_gara=A.gpu_cod_gara and B.gst_progr=A.gpu_progr) 
 			join dussmann.gar_concorrenti_tot as C on (A.gpu_cod_concorrente=C.gco_cod_conco)
 	where B.gst_gruppo  not in ('G3') and B.gst_criterio not in ('PR','SC')
 	group by
		A.gpu_cod_gara,
		A.gpu_progr_ubi,
		A.gpu_cod_concorrente,
		C.gco_nome;
 
quit;

/* data aaa.ds_gare_punteggi; */
/*  set casuser.step_1; */
/* run; */
proc contents data=dussmann.cat_by_gara out=tmp noprint;
run;

proc sql noprint;
 select catx('.','X',name) into: all_cat separated by ', ' from tmp where name like "cat_%";
quit;

%put &all_cat;

/* Dussmann world  CHECKED*/
proc fedsql /*sessref=mySession*/;
create table dussmann.gare_dussmann   as
select distinct
	A.gga_cod_gara,
	A.gga_scad_iscrizione,
	C.gcl_nome,
	A.gga_spec_cliente,
	A.gga_stato,
	E.gst_descr_stato,
	A.gga_gg_durata,
	A.gga_tm_durata,
	A.gga_tm_importo,
	A.gga_tm_oggetto,
	A.gga_tm_categorie,
	X.categorie as categorie_dussmann,
	&all_cat.,
	B.gub_progr,
	B.gub_descr_ubic,
	B.gub_cod_avanzamento,
	D.gsa_descr_stato_av,
	B.gub_data_apertura_amministrativa,
	B.gub_data_apertura_economica,
	B.gub_tm_dt_aggiudicazione,
/* 	dbo.fbi_concatena_settori(gar_gare.gga_cod_gara), */
	case
		when B.gub_importo=0 then A.gga_tm_importo else B.gub_importo end,
	A.gga_settore_dussmann, 
	B.gub_settore_dussmann

	from dussmann.gar_gare as A
	left join dussmann.gar_gare_ubicazioni as B on (A.gga_cod_gara=B.gub_cod_gara)
	left join dussmann.gar_clienti as c on (gga_cliente=gcl_cod_cliente)
	left join dussmann.gar_stato_av as D  on (B.gub_cod_avanzamento=D.gsa_cod_stato_av)
	left join dussmann.gar_stato as E on (gga_stato=gst_cod_stato)
	left join dussmann.cat_by_gara as X on (A.gga_cod_gara=X.gct_cod_gara)
	left join dussmann.gar_gare_categorie as G on (A.gga_cod_gara=G.gct_cod_gara)
	left join dussmann.gar_gare_settori as F on (A.gga_cod_gara=ggs_cod_gara)

	/* controlla i duplicati sulle strutture  da gestire !!!*/
	left join work.struct as Y on 	(Y.gst_cod_gara=A.gga_cod_gara) 
		where Y.gst_cod_gara is not null;
quit;

%put &sysver;


/* data aaa.ds_gare_dumm; */
/*  set casuser.step_2; */
/* run; */

/* non dussmann  */
proc fedsql ;
create table dussmann.gare_no_dusmann  as
select distinct 
	A.gga_cod_gara,
	A.gga_scad_iscrizione,
	C.gcl_nome,
	A.gga_spec_cliente,
	A.gga_stato,
	E.gst_descr_stato,
	A.gga_gg_durata,
	A.gga_tm_durata,
	A.gga_tm_importo,
	A.gga_tm_oggetto,
	A.gga_tm_categorie,
	X.categorie as categorie_dussmann,
	&all_cat.,
	B.gub_progr,
	B.gub_descr_ubic,
	B.gub_cod_avanzamento,
	D.gsa_descr_stato_av,
	B.gub_data_apertura_amministrativa,
	B.gub_data_apertura_economica,
	ifnull(B.gub_tm_dt_aggiudicazione, F.gce_dt_aggiudicazione) as dta_aggiudicazione,
	case
		when B.gub_importo=0 then A.gga_tm_importo else B.gub_importo end,
	
/* 	dbo.fbi_concatena_settori(gar_gare.gga_cod_gara), */
	
	A.gga_settore_dussmann, 
	B.gub_settore_dussmann,
	F.gce_vincitore_ragsoc
	
from dussmann.gar_gare as A

left join dussmann.gar_stato as E on 					(A.gga_stato=E.gst_cod_stato)
left join dussmann.gar_clienti as c on 					(A.gga_cliente=gcl_cod_cliente)
left join dussmann.gar_gare_categorie on 				(A.gga_cod_gara=gct_cod_gara)
left join dussmann.gar_gare_ubicazioni as B on 			(A.gga_cod_gara=gub_cod_gara)
left join dussmann.gar_gare_settori on 					(A.gga_cod_gara=ggs_cod_gara)
left join dussmann.gar_stato_av as D on 				(B.gub_cod_avanzamento=D.gsa_cod_stato_av)
left join dussmann.gar_csv_esiti as F on 				(F.gce_rifgara=A.gga_cod_gara)
/* verifica con  cat_by_gara dedup */
left join dussmann.cat_by_gara as X on 			(A.gga_cod_gara=X.gct_cod_gara)
/* controlla i duplicati sulle strutture  da gestire !!!*/
left join work.struct as Y on 	(Y.gst_cod_gara=A.gga_cod_gara) 
		where Y.gst_cod_gara is null;
quit;

/* data aaa.ds_gare_no_dumm; */
/*  set casuser.step_3; */
/* run; */