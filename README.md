# VRM Quest Middleware

用 FastAPI 把 Quest 上的 VRM 角色连接到 AstrBot。头显通过 WebSocket 发送文字、录音或截图，中转将对话交给 AstrBot，按配置调用语音合成，再把文本、音频和动作返回头显。配套客户端仓库名为 `vrm-quest-unity`。

## 本地启动

准备 Python 3.10+ 和启用了 Open API 的 AstrBot 实例。以下命令在本仓库根目录运行。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
chmod 600 .env
```

编辑 `.env`，填写 AstrBot 的 API 根地址、Key、用户名和目标配置 ID。用 `openssl rand -hex 32` 分别生成 `VRM_TOKEN`（设备）和 `VRM_ADMIN_TOKEN`（管理员）。保留其余示例值即可先测文字。

```bash
python middleware.py
# 在另一终端检查
curl http://127.0.0.1:5310/health
curl http://127.0.0.1:5310/presence/health
```

两条健康检查分别确认中转已启动、观察模块已挂载。上游连通性需实际发一次聊天或图片验证。局域网实测时，将 `MW_HOST` 改为服务器的局域网地址，重启进程。在头显 Settings 填 `ws://服务器局域网IP:5310/ws` 和设备口令。

## 管理与配置

在受信任网络打开 `/admin/astrbot`，输入管理员口令，可修改上游地址、API Key、用户名、Config ID，保存后立即生效。配置影响本实例连接的所有设备。管理页写入本地 `astrbot-settings.json`，下次启动时该文件中的四项优先于 `.env`。请按密钥文件管理它的备份。其余 `.env` 修改在重启时生效。

| 可选功能 | 配置与依赖 |
|---|---|
| 录音转文字 | `pip install faster-whisper`，首次使用需能取得 `small` 模型。当前使用中文转写。 |
| 文本转语音 | 单独部署 GPT-SoVITS，设置 `TTS_ENABLED=true`、`GPT_SOVITS_API` 和 `GPT_SOVITS_REF_*`。参考音频需要 TTS 服务能读取。 |
| 自动观察 | 使用已有 AstrBot 的 `/file` 和 `/chat`，选择支持图片的模型。独立视觉会话接收帧，主会话可读取近期描述。 |
| 扩展动作 | `HAST_PLAN_URL` 和 `HAST_BRIDGE_URL` 可接外部服务。有权使用的四元数资源放在 `body_model/model.json` 和 `skills_quat.json`。基础服务可在没有动作数据时启动。 |

## 接口与部署边界

| 路径 | 用途 |
|---|---|
| `/ws` | 头显双向消息，包含文本、语音和手动截图 |
| `/presence/observe`、`/presence/event`、`/presence/context` | 观察帧、动作事件和近期上下文，校验 `X-VRM-Token` |
| `/admin/astrbot` | 浏览器管理页面，保存时校验管理员口令 |
| `/health`、`/presence/health` | 本地诊断 |

**部署范围默认选本机或可信局域网。** `/ws` 与调试接口 `/test` 目前没有用户鉴权。远程使用请通过私有 VPN，或在网关实现与客户端兼容的访问控制，管理页限制给管理员，公开传输使用 HTTPS/WSS。多人共用时需另行设计身份与会话隔离。开启观察或录音会把现实画面、语音或转写内容发送给所配置的服务。发布前告知使用者处理目的与数据去向，检查日志与 AstrBot 的保留策略。

## 发布范围

本仓库包含中转源码与配置示例。AstrBot、GPT-SoVITS、语音模型、动作模型请从各自项目获取并遵守其许可。仓库自有代码采用 MIT 许可证。

## HAST-SEC 来源与许可

动作系统集成参考 [HAST-SEC v0.1.1](https://github.com/sola1039/HAST-SEC/tree/v0.1.1)。Copyright (c) 2026 HAST-SEC 项目贡献者。

本仓库的适配工作：中转端通过可选的规划和动作接口接入 HAST，读取四元数技能帧并通过 WebSocket 转发至 Quest。

HAST-SEC 原创部分及其修改版遵循[非商业学习与研究许可证 0.1](third_party/HAST-SEC/LICENSE)。非商业再分发须保留来源、许可和第三方声明，标明修改内容。商业使用须事先取得权利人书面授权。[商用授权联系渠道](https://github.com/sola1039/HAST-SEC/issues)

仓库根目录的 MIT 许可证仅适用于本仓库独立原创代码。上游使用的动作数据、角色、模型和软件各自遵循其原始许可，具体来源和引用见[上游第三方资源声明](third_party/HAST-SEC/THIRD_PARTY_NOTICES.txt)。
