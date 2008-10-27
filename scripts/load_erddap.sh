wget -q http://amoeba.ucsd.edu/fileshare/apps/erddap-ooi-latest.tar.gz 
tar zxf erddap-ooi-0.1.tar.gz
mv erddap-ooi /opt/apache-tomcat-6.0.18/webapps/
mkdir /opt/apache-tomcat-6.0.18/content
cp -r /opt/apache-tomcat-6.0.18/webapps/erddap-ooi/content.def/erddap /opt/apache-tomcat-6.0.18/content
ln -s /opt/apache-tomcat-6.0.18/webapps/erddap-ooi/ /opt/apache-tomcat-6.0.18/webapps/erddap

