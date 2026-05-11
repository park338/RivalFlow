# RivalFlow Demo

一个可直接跑通的多 Agent 竞品分析 Demo，支持从任务提交到报告生成的完整流程演示。

## 项目结构

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
run_demo.ps1           # 一键启动脚本
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

或在项目根目录运行：

```powershell
.\run_demo.ps1
```

3. 打开页面

- [http://localhost:8000](http://localhost:8000)
- API 文档：[http://localhost:8000/docs](http://localhost:8000/docs)

## 当前能力（MVP）

1. 创建分析任务（行业、竞品、维度、时间范围）
2. 自动执行流程：`Planner -> Collector -> Structurer -> Analyst -> Reviewer -> Reporter`
3. 实时查看节点状态和流程日志
4. 输出证据链、评分卡、核心结论和 Markdown 报告
