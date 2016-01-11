# [MockMySQLServer](https://github.com/MyCATApache/Mycat-NIO/blob/master/src/main/java/io/mycat/net2/mysql/MockMySQLServer.java)

一个基于新版mycat-nio框架的mysql server的mock实现，用于跟进mycat-nio框架及MySQL协议的研究。

## 使用方法

入口类：[io.mycat.net2.mysql.MockMySQLServer](https://github.com/MyCATApache/Mycat-NIO/blob/master/src/main/java/io/mycat/net2/mysql/MockMySQLServer.java)

监听端口：8066

目前MockServer只支持登录认证、select语句、exit

##### 登录：mysql -uroot -proot -P 8066 （是判断用户名是否为root）

##### select查询，永远返回写死的fake数据

##### exit  释放连接