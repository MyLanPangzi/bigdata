rpm -qa | grep -i -E mysql\|mariadb | xargs -n1 sudo rpm -e –-nodeps
sudo systemctl start mysqld
sudo cat /var/log/mysqld.log | grep password
sudo mysql -uroot -p 'fPQ:/eKSF3-1'
set password=password("Qs23=zs32");
set global validate_password_length=4;
set global validate_password_policy=0;
set password=password("000000");
use mysql;
select user, host from user;
update user set host="%" where user="root";
flush privileges;
quit;
sql="
select id,
       user_id,
       app_name,
       title,
       coin,
       coin_source,
       ct
from matrix_idiom_master_coin_detail
where ct between date_format('yyyyMMdd HH:mm:ss', '${dt} ${hour}:00:00')
    and date_format('yyyyMMdd HH:mm:ss', '${dt} ${hour}:59:59')
"