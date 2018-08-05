Sql output multitple tables for top store sales

github
https://tinyurl.com/y6u3dz5w
https://github.com/rogerjdeangelis/utl_sql_output_multitple_tables_for_top_store_sales

see
https://communities.sas.com/t5/Base-SAS-Programming/how-to-create-variable-in-sql/m-p/484079
https://communities.sas.com/t5/Base-SAS-Programming/how-to-create-variable-in-sql/m-p/484079

Important note. The Ops business rules imply thta there are only 3 vales for variable Ord,
otherwise the provided code would fai.

13 split table techniques;
https://tinyurl.com/y6uzqbm2
https://github.com/rogerjdeangelis/utl_thirteen_algorithms_to_split_a_table_based_on_groups_of_data


INPUT
=====

WORK.HAVE total obs=12

                          |   RULES  (If ( last ID x Ord ) sales is >= 90 then
                          |   -----   then ouput table named sales.ord)
Obs    ID    ORD    SALES |   Totals
                          |
  1    A      1        3  |
  2    A      1       58  |    61
                          |
  3    A      2       40  |
  4    A      2       62  |   102    > 90 and ord=2 output table_2
                          |
  5    A      3       71  |
  6    A      3       93  |   164    > 90 and ord=3 output table_3
                          |
  7    B      1       82  |
  8    B      1       13  |    94    > 90 and ord=1 output table_1
                          |
  9    B      2       37  |
 10    B      2       50  |    87
                          |
 11    C      3       34  |
 12    C      3       99  |   133    > 90 and ord=3 output table_3


 EXAMPLE OUTPUT
 --------------

 TABLE_1 total obs=1

 Obs    ID    ORD    TOTSALE

  1     B      1        95


 TABLE_2 total obs=1

 Obs    ID    ORD    TOTSALE

  1     A      2       102


 TABLE_3 total obs=2

 Obs    ID    ORD    TOTSALE

  1     A      3       164
  2     C      3       133



PROCESS
=======

* meta data;
%array(ord,values=1 2 3)
%array(dat,values=Ret Cat Inc)

proc sql;

  * split into three tables;
  %do_over(ord dat,phrase=%str(
      create
         table want?dat as
      select
         *
      from
         havSum
      where
         ord=?ord
      ;)
  )
  * get filtered totals ie sales >=90;
  create
     table havSum as
  select
     id
    ,ord
    ,sum(sales) as totSale
  from
    have
  group
    by id, ord
  having
    totSale >= 90
;quit;


OUTPUT
======

 WANTREC total obs=1

 Obs    ID    ORD    TOTSALE

  1     B      1        95


 WANTCAT total obs=1

 Obs    ID    ORD    TOTSALE

  1     A      2       102


 WANTINC total obs=2

 Obs    ID    ORD    TOTSALE

  1     A      3       164
  2     C      3       133

*                _              _       _
 _ __ ___   __ _| | _____    __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \  / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/ | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|  \__,_|\__,_|\__\__,_|

;

data have;
  do id="A","B","C";
   do ord=1, 1, 2, 2, 3, 3;
     sales=ceil(100*uniform(5731));
     select;
        when (id="A")           output;
        when (id="B" and ord<3)  output;
        when (id="C" and ord=3)  output;
        otherwise;
     end;
   end;
 end;
run;quit;

*          _       _   _
 ___  ___ | |_   _| |_(_) ___  _ __
/ __|/ _ \| | | | | __| |/ _ \| '_ \
\__ \ (_) | | |_| | |_| | (_) | | | |
|___/\___/|_|\__,_|\__|_|\___/|_| |_|

;

* als see process

proc datasets lib=work;
delete want: ;
run;quit;


%array(ord,values=1 2 3)
%array(dat,values=Ret Cat Inc)

proc sql;
  %do_over(ord dat,phrase=%str(
      create
         table want?dat as
      select
         *
      from
         havSum
      where
         ord=?ord
      ;)
  )
  create
     table havSum as
  select
     id
    ,ord
    ,sum(sales) as totSale
  from
    have
  group
    by id, ord
  having
    totSale >= 90
;quit;

*                            _       _   _
  ___  _ __  ___   ___  ___ | |_   _| |_(_) ___  _ __
 / _ \| '_ \/ __| / __|/ _ \| | | | | __| |/ _ \| '_ \
| (_) | |_) \__ \ \__ \ (_) | | |_| | |_| | (_) | | | |
 \___/| .__/|___/ |___/\___/|_|\__,_|\__|_|\___/|_| |_|
      |_|
;


data wantRet wantCat WantInc;
   set have;
   by ID Ord;
   if first.Ord then totSale=0;
   TotSale+Sales;
   put id= totSale=;
   if last.Ord and totSale >= 90 then do;
      select (Ord);
           when (1) output wantRet;
           when (2) output wantCat;
           when (3) output wantInc;
        end;
   end;
run;quit;



