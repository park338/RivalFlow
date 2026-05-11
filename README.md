# RivalFlow Demo

可运行的多 Agent 竞品分析 Demo，支持任务提交、流程执行、证据追溯、报告生成。

## 快速启动

```powershell
cd backend
python -m pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

或在项目根目录执行：

```powershell
.\run_demo.ps1
```

打开：

- [http://localhost:8000](http://localhost:8000)
- [http://localhost:8000/docs](http://localhost:8000/docs)

## 当前能力（MVP）

1. 分析维度、时间范围支持下拉选择。
2. Analyst 接入 `deepseek-v4-flash` 进行分析（可由 `DEEPSEEK_API_KEY` 环境变量覆盖）。
3. 任务进度与流程日志展示节点上下文（模型、耗时、token、输入摘要）。
4. 每条结论强制绑定证据 ID，保证可追溯。
5. 提供明确样例：`抖音 / 快手 / 小红书`。
