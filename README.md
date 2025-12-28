# embyrename (V2.12)

一个能直接跑在 **Ubuntu/VPS** 上的 AList 电视剧整理/重命名工具（面向 Emby/Jellyfin 刮削）。

核心原则（按你的要求）：

- **文件名里只要已经有 `SxxEyy`：默认不改文件名**（但可能会移动到正确的 `S01/S02...` 目录）。
  - 例外：如果文件名包含明显广告/站点/引流标记（如 `www`、`http`、`公众号`、`关注` 等），会强制清理并改成标准命名。
- 如果文件名只有 `SxxEyy` 没有剧名：可选自动补齐成 `剧名 - SxxEyy`，保证能刮削。
- 目录命名按 Emby 习惯：`剧名 (年份)/S01/...`
- 自动跳过广告/福利/海报/花絮/无关目录（可配置）。
- **清理广告文件默认开启**：会删除常见无关文件（`pdf/doc/docx/xls/xlsx/ppt/pptx/url/lnk/html/htm/torrent` 等）与明显广告目录。
  - **不会删除 `.txt`**（按你的要求）。
- **遇到难处理、夹杂大量噪声的文件夹名**：可选交给 AI 先提取“干净剧名”，再去 TMDB 搜索，提高命中。

---

## 1) 一键开始（推荐）

> ✅ 如果你希望“配置/日志”放到独立目录（例如 /opt/embyrename），只需在系统环境里设置一次：
>
> ```bash
> export EMBYRENAME_CONFIG_DIR=/opt/embyrename
> ```
>
> 之后所有命令都直接 `./embyrename ...` 跑，不需要再写 `env XXX=...` 前缀。

```bash
cd /AAAaaliyy/embyrename_bundle
chmod +x ./embyrename
./embyrename setup
```

`setup` 会：
1) 创建 `.venv` 并安装依赖
2) 生成 `.env`（如果不存在，会从 `.env.example` 复制）
3) 用 AList 列目录自动发现 `TV_ROOTS`（只找常见的 `电视剧/动漫`），避免你手动填路径

> 你也可以手动编辑 `.env`，把 `ALIST_URL / ALIST_TOKEN / TV_ROOTS` 填好即可。

---

## 2) 先搜（不改名，低风控）

当你想确认 AList 能看到某个剧文件夹：

```bash
./embyrename search "他为什么依然单身"
```

只会调用 **一次** AList 的目录/搜索（不会递归大扫），输出匹配路径。

---

## 3) 单剧测试：预演 + 执行

```bash
# 预演（不会改任何东西）
./embyrename plan "他为什么依然单身" --ui

# 确认无误后执行
./embyrename apply "他为什么依然单身" --ui
```

打开网页日志（默认端口 53943）：
- 仅本机：`http://127.0.0.1:53943`  
- 公网访问：把 `.env` 里 `LOG_HOST=0.0.0.0`，然后访问：`http://你的VPSIP:53943/?token=xxx`

---

## 4) 批量处理（不需要写 --only）

只要你 `.env` 里设置好了 `TV_ROOTS`（**只放电视剧根目录**），就可以直接批量跑：

```bash
# 预演批量
./embyrename plan --ui

# 批量执行
./embyrename apply --ui
```

它只会遍历 `TV_ROOTS` 的 **第一层子目录**（每个子目录当作一个“剧根目录”），不会全库递归，API 压力很小。

---

## 5) 后台运行（你关电脑也不影响）

推荐用 nohup（非交互，直接执行 apply）：

```bash
cd /AAAaaliyy/embyrename_bundle
nohup ./embyrename apply --yes --ui > "${EMBYRENAME_CONFIG_DIR:-${EMBYRENAME_HOME:-.}}/logs/nohup.out" 2>&1 &
tail -f "${EMBYRENAME_CONFIG_DIR:-${EMBYRENAME_HOME:-.}}/logs/nohup.out"
```

或者用脚本：

```bash
./embyrename daemon
```

> `daemon` 内部就是 `apply --yes --ui`，不会卡在确认提示。

---

## 6) 回滚（Undo）

每次 **apply** 会在 `${EMBYRENAME_CONFIG_DIR:-${EMBYRENAME_HOME:-<bundle>}}/logs/undo-*.jsonl` 记录回滚信息（可在 `.env` 配置 `UNDO_FILE` 固定路径）。

回滚：

```bash
./embyrename undo "${EMBYRENAME_CONFIG_DIR:-${EMBYRENAME_HOME:-.}}/logs/undo-20251225-010203.jsonl" --yes
```

---

## 7) 你关心的特殊情况支持

- **“中文第4季26集” => Season 4**：已做映射（`第4季`、`第四季`、`Season 4`、`S4`、`S04` 等都会识别为 Season 4）。
- **散落的 S04**：如果某些文件夹里混着 `S04/S4/第四季/第4季` 相关内容，会自动归并到 `S04` 文件夹。
- **字幕同名**：当视频文件会被重命名（比如 e01.mp4 -> 剧名 - S01E01.mp4），对应 `srt/ass/ssa` 也会同步改名。
- **/字幕 目录**：如果字幕被单独放在 `剧名/字幕`（或 `subs/subtitles`）下面，会自动移动到与视频文件同级目录，再按 Emby/Plex 标准改名（例如 `...S01E01.en.srt`）。
- **质量尾巴保留**：`2160p/1080p/HDR/DV/Dolby` 等会尽量保留；并把 `4k` 统一成 `4K`（不会删掉你已有的尾巴）。

---

## 8) .env 变量说明（最常用）

- `ALIST_URL`：AList 地址
- `ALIST_TOKEN`：Token
- `TV_ROOTS`：电视剧/动漫根目录（多个逗号分隔）
- `TMDB_KEY`：TMDB API Key
- `TMDB_API_BASE`：TMDB API Base（被墙可用镜像/代理）
  - 官方：`https://api.themoviedb.org` -> 自动补 `/3`
  - 代理：`https://tmdb.melonhu.cn` -> 自动补 `/get`（同时支持 `/img` 映射到官方图片域 `https://image.tmdb.org/`）
  - 若你显式写了 `/3` 或 `/get`，程序保持不变
- `AI_BASE_URL / AI_API_KEY / AI_MODEL`：AI 网关（兼容 `/v1/chat/completions`），留空则完全不调用 AI
- `LOG_HOST / LOG_PORT / LOGUI_TOKEN`：日志网页服务（公网请配 token）
- `ON_CONFLICT`：重名冲突策略（`suffix` 或 `skip`）
- `ALIST_REFRESH`：是否对目录调用 refresh（OneDrive 容易 500，默认 0）
- `ALIST_SLEEP_READ / ALIST_SLEEP_WRITE`：AList 读/写请求间隔
- `ALIST_RETRIES`：AList 失败重试次数
- `STATE_FILE`：断点续跑状态文件（jsonl）
- `UNDO_FILE`：回滚日志文件（jsonl）

---

## 9) 常见报错：AList object not found / 500

如果你遇到：

```
failed get objs: failed get dir: object not found
```

通常是 OneDrive provider 在 refresh/并发下短暂不一致。

解决：
1) 确认 `.env` 里 `ALIST_REFRESH=0`（默认就是 0）
2) 把 `ALIST_SLEEP_READ/WRITE` 调大，例如：
   - `ALIST_SLEEP_READ=0.8`
   - `ALIST_SLEEP_WRITE=1.2`
3) 让它自动重试（默认 `ALIST_RETRIES=5`）

---

## 10) 免责声明

本项目只对“电视剧/动漫根目录”运行，请不要把电影根目录塞进去。

