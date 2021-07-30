# flink-dos-detection
## 简介
解析并聚合功能端发出的疑似DDoS攻击日志，基于静态阈值与基线判断是否为DoS攻击。

输入：
DosSketchLog，功能端发出的TopN日志
输出：
DosMetricsLog: 目标IP流量统计日志；
DosEventLog: Dos事件日志,即最终检查结果

