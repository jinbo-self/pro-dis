启动命令：make all
将文件打包到bin命令下，此项目仅在windows10环境下测试过，需要注意Linux环境下可能会报错。
启动的时候一共会开6个数据节点，1个元数据节点和一个客户端，在打包到bin目录下后，请将dataserver.exe复制到6个dataserver目录中

启动时，先启动dataserver.exe，请使用dataserver1到6目录下的批处理命令启动，

然后再启动bin目录下的metaserver.bat，用来启动元数据服务

最后启动客户端，启动时请打开cmd窗口，然后用命令行启动，.\client.exe -c put -f 1.png

-c是启动的命令类型，暂时只有三个，put/del/get，-f后面跟文件名，请将想要put/get/del的文件放入bin目录，

