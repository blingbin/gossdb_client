# gossdb_client
ssdb client



考虑到从连接池获取client可能会进行多个命令操作，因此未单独将每个命令封装连接池的Put，Get操作。业务中可以参考example进行二次封装。能力有限，欢迎提出bug和改进建议。