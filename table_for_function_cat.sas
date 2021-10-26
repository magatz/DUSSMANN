proc sql;
	create table work.t1 as select distinct gct_cod_gara, gct_cod_dussmann from 
		dussmann.gar_gare_categorie where not missing(gct_cod_dussmann);
quit;

proc sort data=t1 out=t1_dedup nodupkey dupout=t1_dup;
	by gct_cod_gara gct_cod_dussmann;
run;

data work.t2 (keep=gct_cod_gara categorie);
	set work.t1_dedup;
	length categorie $500;
	by gct_cod_gara;
	retain categorie;

	if first.gct_cod_gara and last.gct_cod_gara then
		categorie=gct_cod_dussmann;

	if first.gct_cod_gara and not last.gct_cod_gara then
		categorie=gct_cod_dussmann;
	else if not first.gct_cod_gara or last.gct_cod_gara then
		categorie=catx(" | ", categorie, gct_cod_dussmann);
run;

proc transpose data=t1_dedup prefix=cat_dussmann_ out=cat_by_gara(drop=_name_ _label_);
	by gct_cod_gara;
	var gct_cod_dussmann;
run;

data dussmann.cat_by_gara;
	merge t2 cat_by_gara;
	by gct_cod_gara;
run;

proc sort data=dussmann.cat_by_gara nodupkey;
by gct_cod_gara;
run;




proc sql;
	create table work.t1b as select distinct  gga_cod_gara, gga_settore_dussmann from 
		dussmann.gar_gare;
quit;

proc sort data=work.t1b out=t1b_dedup dupout=t1b_dup nodup;
 by gga_cod_gara gga_settore_dussmann;
run;


data work.t2b (keep=gga_cod_gara gga_settore_dussmann settori);
	set work.t1b_dedup;
	length settori $500;
	by gga_cod_gara;
	retain settori;

	if first.gga_cod_gara and last.gga_cod_gara then
		settori=gga_settore_dussmann;

	if first.gga_cod_gara and not last.gga_cod_gara then
		settori=gga_settore_dussmann;

	else if not first.gga_cod_gara  then
		settori=catx(" | ", settori, gga_settore_dussmann);
run;

proc transpose data=t1b_dedup prefix=cat_dussmann_ out=sett_by_gara(drop=_name_ _label_);
	by gga_cod_gara;
	var gga_settore_dussmann;
run;


proc sort data=dussmann.gar_gare_struttura nodupkey out=work.struct;
by gst_cod_gara;
run;