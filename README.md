# CandyFS

CandyFS is a distributed file system that provides high availability and performance.

## 功能特点

- 高可用性：支持多副本存储，保证数据不丢失
- 高性能：优化的数据分布和传输机制
- 易用性：简单直观的命令行界面
- 兼容性：支持多种存储和访问方式

## 安装方法

```bash
# 从源代码构建
go build -o candyfs

# 或安装到系统
go install
```

## 命令参考

CandyFS提供了直观的命令行工具，支持以下功能：

### 基础命令

```bash
# 显示版本信息
candyfs version

# 全局选项（适用于所有命令）
candyfs --verbose   # 显示详细输出
```

### 服务管理

```bash
# 启动服务
candyfs start
candyfs start --port 9000 --config ./config.yaml

# 停止服务
candyfs stop

# 查看服务状态
candyfs status
```

### 存储管理

```bash
# 格式化存储（首次使用前必须执行）
candyfs format --path /data/storage

# 强制格式化（会清除已有数据）
candyfs format --path /data/storage --force
```

## 配置示例

```yaml
storage:
  paths:
    - /data/storage1
    - /data/storage2
  replication: 3

network:
  listen: 0.0.0.0
  port: 8080
  
logging:
  level: info
  path: /var/log/candyfs
```

## 许可证

MIT License
