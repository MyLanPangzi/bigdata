schematool -initSchema -dbType mysql -verbose
nohup hive --service metastore >/dev/null 2>&1 &
#https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration
#https://cwiki.apache.org/confluence/display/Hive/Hive+Schema+Tool
#https://cwiki.apache.org/confluence/display/Hive/Home#Home-HiveDocumentation