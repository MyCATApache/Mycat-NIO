# [MockMySQLServer](https://github.com/MyCATApache/Mycat-NIO/blob/master/src/main/java/io/mycat/net2/mysql/MockMySQLServer.java)

一个基于新版mycat-nio框架的mysql server的mock实现，用于跟进mycat-nio框架及MySQL协议的研究。

## 使用方法

入口类：[io.mycat.net2.mysql.MockMySQLServer](https://github.com/MyCATApache/Mycat-NIO/blob/master/src/main/java/io/mycat/net2/mysql/MockMySQLServer.java)

入口类中的静态方法快配置了一个DB node，目前大部分报文会直接转发到这个node上进行查询，返回的报文直接传给client

监听端口：8066

目前MockServer只支持登录认证、大部分CRUD语句、exit

##### 登录：mysql -uroot -proot -P 8066 （是判断用户名是否为root）

##### 查询

##### exit  释放连接