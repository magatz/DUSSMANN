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
	create table work.t1b as select distinct   ggs_cod_gara, ggs_cod_settore from 
		dussmann.gar_gare_settori;
quit;

proc sort data=work.t1b out=t1b_dedup dupout=t1b_dup nodupkey;
 by ggs_cod_gara ggs_cod_settore;
run;

proc sort data=dussmann.gar_gare_settori dupout=pluto nodupkey out=pippo;
 by ggs_cod_gara ;
run;


data work.t2b (keep=ggs_cod_gara ggs_cod_settore settori );
	set work.t1b_dedup;
	length settori $500;
	by ggs_cod_gara;
	retain settori;

	if first.ggs_cod_gara and last.ggs_cod_gara then
		settori=ggs_cod_settore;

	if not first.ggs_cod_gara then
		settori=catx(" | ", settori, ggs_cod_settore);
run;

proc transpose data=t2b prefix=sett_dussmann_ out=sett_by_gara(drop=_name_ _label_);
	by ggs_cod_gara;
	var ggs_cod_settore;
run;


