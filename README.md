# spark-workshop
spark-workshop

git clone https://github.com/Talentica/spark-workshop.git

Non-ubuntu, MAC os users follow below mentioned steps
1) Open terminal window.Go to folder spark-workshop

2) Type command vagrant up. This will download VM for you

3) Type command vagrant provision. This will setup all software required

4) Type command vagrant ssh to connect to VM

Ubuntu users just run ./setup-deb-machine.sh from spark-workshop folder

Windows users follow Setup document Steps To Set Up VM on your VirtualBox.docx

Note: If you get this error while running apt-get commands - "Could not connect to archive.ubuntu.com", follow these steps - 

1) sudo vi /etc/resolv.conf
2) Comment out the original value of nameserver
3) Use nameserver value as 8.8.8.8 or 8.8.4.4
4) Save the file, and try your command again.
