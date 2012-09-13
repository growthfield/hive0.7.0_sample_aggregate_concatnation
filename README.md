# Aggregate concatnation with sorting

HiveのUDAF勉強用サンプル。
Oracle 10gの隠し関数 wm_concatや、11gのLISTAGG相当のHive UDAFです。

    hive> add jar /path/to/listagg-1.0.jar
    hive> create temporary function listagg as 'jp.growthfield.hive.udaf.GenericUDAFListAgg';
    hive> select id, listagg(value_col, sort_col) from foo_table group by id;

第一引数であるvalue_colカラムの値をカンマ区切りの文字列として出力します。
第二引数は任意で、指定するとその値でソートをします。

