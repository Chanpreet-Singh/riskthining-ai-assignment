folderName='chanpreet_vm'
mkdir $folderName
cd $folderName
wget https://downloads.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
sudo tar -xf spark*.tgz
cd ..
sudo chmod -R 777 ./chanpreet_vm/

currentFolder=$(pwd)
slash='/'
sparkFolder=$currentFolder$slash$folderName$slash"spark-3.4.0-bin-hadoop3"
echo "export SPARK_HOME="$sparkFolder >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> ~/.bashrc
sleep 3s
source ~/.bashrc
