
### 原则

保持结构清晰，代码可读性
简化

### 日志
cache common模块无日志 cache有状态统计
记录系统配置文件的加载情况 各服务启动和停止状态
可触发报警


receiver |
cache |
whisper |
api

receiver
---
g| active_conn

c| point-bag-received

c| point-received

cache
-----
g| point-count
c| query-times
c| overflow-count
c| queue-build-times

whisper
-------
g| metric-count
c| metric-create


每次写时间 和数量

每次查找时间 和数量