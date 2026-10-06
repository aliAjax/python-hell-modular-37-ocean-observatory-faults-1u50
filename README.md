# 海底观测网设备故障管理

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8337`。领域对象包括站点、资产、链路、遥测、故障事件、恢复动作、出海任务和数据缺口。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常（含批次校验错误）、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、冲突、跨对象校验，以及离线批次校验、版本归类和事件失效重算等纯判定逻辑。
- `src/merge.py`：离线合并编排：整批校验、多版本留存、遥测修订号应用、事件重开和后到提交差异视图。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计、幂等键、离线批次与版本存储。
- `src/service.py`：用例编排，把HTTP层接到判定/合并/存储三个模块。
- `src/http_api.py`：HTTP路由、JSON解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则、失败场景和离线合并（含并发）测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8337
```

服务启动时自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。初始化服务不需要单独命令，首次启动即可访问：

```bash
curl http://127.0.0.1:8337/health
```

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `POST /api/offline-records`（别名`/api/offline-merge`）：提交离线批次`{"batch_id":"可选,默认按内容哈希","records":[...]}`。
- `GET /api/offline-batches/<batch_id>`：查询批次（含失败批次的错误位置和重试后的合并结果）。
- `GET /api/offline-records/<record_id>/versions`：读取同一逻辑记录保留的全部版本。
- `GET /api/audit`：读取审计记录。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。## 核心流程

建立站点、资产和链路后记录遥测与故障事件，创建恢复动作并跟踪重启、备用链路、出海任务和数据缺口，最后关闭事件。遥测`revise`动作只接受更高修订号，用于处理迟到数据。

## 规则重点

- 同一资产和故障类型不能同时有多个活动事件。
- 恢复动作按`dedupe_key`防止重复执行。
- 事件解决前恢复动作、数据缺口和受影响资产必须达到可关闭状态。
- 事件解决时快照当时各遥测序列的修订号（`resolved_revisions`）和解决时间（`resolved_at`）。

## 离线合并与事件失效重算

外海断链期间在船上登记的记录回岸后按批次合并，判定、合并、存储分属`rules.py`、`merge.py`、`repository.py`三个模块：

- **两版留存、后到不覆盖**：恢复记录按`record_id`归并，先入库的版本永久为canonical；船端/岸端对同一记录的不同修改按`source`和`recorded_at`各存一版，后到的标记为`conflict`保留，绝不覆盖先前改动；完全相同的重复提交记为`duplicate`，不产生新版本。
- **事件失效重算**：合并时若收到高于解决快照修订号的遥测，或离线恢复记录的`recorded_at`晚于事件`resolved_at`，已`resolved`/`closed`的事件自动`reopen`回`open`，结果中给出每个事件的触发原因和受影响资产清单；遥测序列本身写入更高修订号。
- **整批原子、错误定位**：批次先做全量校验，任一条失败则整批不入库，响应为400并逐条给出`index`、`record_id`、`field`、`message`；失败批次持久化为`failed`，用同一`batch_id`修正后重提即可重试，成功后转`merged`。
- **先到生效**：批次默认可带显式`batch_id`（不带则按内容哈希生成），合并在单个`BEGIN IMMEDIATE`事务内完成。两名值班员同时提交同一批时，先到者写入，后到者拿到已合并结果和`diff`（恢复记录逐字段差异、遥测修订号same/behind/newer对比），不会重复入库。
- 遥测离线条目要求主库已存在同资产同指标序列，且修订号必须严格高于当前值（批次内也必须递增）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
