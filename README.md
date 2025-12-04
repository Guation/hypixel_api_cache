# HYPIXEL API 缓存

## 如何使用

1. `Python3.9` 以上版本。

2. 复制项目`git clone https://github.com/Guation/hypixel_api_cache.git & cd hypixel_api_cache`

3. 安装venv环境库`apt update & apt install python3-venv`

4. 生成venv环境`python3 -m venv venv`

5. 进入venv环境`source venv/bin/activate`

6. 安装依赖`pip install aiohttp zstandard`

7. 退出venv环境`deactivate`

8. 将以下内容填入`run.sh`文件作为启动脚本，其中`01234567-89ab-cdef-fedc-ba9876543210`替换为您从[Hypixel开发者控制面板](https://developer.hypixel.net/dashboard)获取的key
```shell
#!/bin/bash
cd `dirname $0`
cd src
export HYPIXEL=01234567-89ab-cdef-fedc-ba9876543210
../venv/bin/python3 main.py
```

9. 为run.sh添加启动权限`chmod +x run.sh`

10. 以后您可以使用`./run.sh`启动本项目

11. 本项目默认监听`http://127.0.0.1:8001`，您可以使用`nginx`未项目添加`https`加密
```nginx
server {
        listen 443;
        listen [::]:443;
        server_name example.com;
        ssl_certificate ssl/full_chain.pem;
        ssl_certificate_key ssl/private.key;
        ssl_session_cache shared:SSL:5m;
        ssl_session_timeout 5m;
        ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE:ECDH:AES:HIGH:!NULL:!aNULL:!MD5:!ADH:!RC4;
        ssl_protocols TLSv1.2 TLSv1.3;
        ssl_prefer_server_ciphers on;
        charset utf-8;
        location / {
                proxy_pass http://127.0.0.1:8001/;
                proxy_http_version 1.1;
                proxy_buffering off;
                proxy_request_buffering off;
        }
}
```

## 从旧版本迁移数据库

```shell
cd src
../venv/bin/python3 database_migrate.py
```
