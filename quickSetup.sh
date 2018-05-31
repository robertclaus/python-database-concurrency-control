echo "When prompted use password 'test' and select the legacy authentication mode."

sudo apt-get purge mysql-server mysql-client mysql-common mysql-server-core-* mysql-client-core-*
sudo rm -rf /etc/mysql /var/lib/mysql
sudo apt-get autoremove
sudo apt-get autoclean
cd /opt
sudo wget https://dev.mysql.com/get/mysql-apt-config_0.8.10-1_all.deb
sudo dpkg -i mysql-apt-config_0.8.10-1_all.deb
sudo apt-get update
sudo apt-get -y install mysql-server
sudo apt-get -y install libmysqlclient20
sudo service mysql start
sudo mysql -ptest -e "CREATE DATABASE mydb"
export PYTHONPATH='.'
cd /opt/tatpbenchmark-1.1.1/source/src/