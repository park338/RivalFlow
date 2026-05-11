# Bytedance Multi-Agent 竞品分析 Demo

这是一个可直接跑通的前后端 Demo，目标是演示：

1. 用户提交竞品分析任务
2. 多 Agent 流程自动执行
3. 输出证据链、评分卡、结论和报告

## 目录结构

```text
backend/
  app/
    main.py            # FastAPI 入口
    models.py          # 数据模型
    storage.py         # 内存任务存储
    pipeline.py        # 多 Agent 执行流程
    static/
      index.html       # 前端页面
      app.js
      styles.css
  requirements.txt
```

## 快速启动

1. 安装依赖

```powershell
cd backend
python -m pip install -r requirements.txt
```

2. 启动服务

```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

或在项目根目录直接运行：

```powershell
.\run_demo.ps1
```

3. 打开页面

- [http://localhost:8000](http://localhost:8000)
- API 文档：[http://localhost:8000/docs](http://localhost:8000/docs)

## 当前能力（MVP）

1. 创建任务：行业、竞品、维度、时间范围
2. 流程编排：Planner -> Collector -> Structurer -> Analyst -> Reviewer -> Reporter
3. 实时状态：节点状态、流程日志轮询展示
4. 结果输出：证据列表、结构化评分卡、结论与 Markdown 报告

## 为什么没接 MySQL

当前版本为了保证一键跑通与演示稳定，使用内存存储，不依赖外部数据库。
下一版如果你要上线或多用户并发，再切 MySQL（任务表、节点表、证据表、报告表）即可。

## 你后续可能需要提供的信息

如果进入下一版（真实抓取 + 持久化 + 部署），建议你准备：

1. MySQL 地址、端口、用户名、密码、库名
2. 模型服务地址（如豆包/DeepSeek/OpenAI 兼容网关）和 API Key
3. 部署环境（Linux/Windows、容器或裸机、域名）
