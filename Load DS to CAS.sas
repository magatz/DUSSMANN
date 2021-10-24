/* cas mysession terminate; */
options casdatalimit=ALL;
cas mysession sessopts=(caslib=casuser timeout=1800);
caslib _all_ assign;

proc cas;

	table.addCaslib / name="dussmann" description="Dussman Data" 
		dataSource={srctype="path"} path="/mnt/storage/sasdata/DUSSMANN/";

	table.fileInfo result=fileList / kbytes=true;

	do row over fileList.FileInfo;
		print row.name;
		table.loadtable / path=row.name, caslib='dussmann', casout={caslib='dussmann', 
			name=substr(row.name , 1, index(row.name, '.')-1), replace=true};
	end;
quit;

libname dussmann cas caslib=dussmann;

/*
proc fedsql sessref=mysession;

 create table casuser.aggiudicatari as
select
	gpu_cod_gara,
	gpu_progr_ubi,
	gpu_cod_concorrente,
	gco_nome,
	sum(gpu_punteggio) 
from
	dussmann.gar_gare_punteggi as A join dussmann.gar_gare_struttura as B

 		on B.gst_cod_gara= A.gpu_cod_gara and B.gst_progr=A.gpu_progr 

 			join dussmann.gar_concorrenti as C on (A.gpu_cod_concorrente=C.gco_cod_conco)

 where B.gst_gruppo not in ('G3') and B.gst_criterio not in ('PR','SC')

 group by
	A.gpu_cod_gara,
	A.gpu_progr_ubi,
	A.gpu_cod_concorrente,gco_nome;
quit;
*/