"""
VRM Quest 中间件
================
连接 AstrBot 和 Quest 3 的桥接服务器

架构:
    Quest 3 (Godot)  ←WebSocket→  本中间件  ←HTTP/WS→  AstrBot API

职责:
    1. 接收 Quest 端的文字/语音输入
    2. 转发给 AstrBot 获取回复文本
    3. 情绪分析 → emotion
    4. 生成骨骼动作队列 → motion
    5. TTS 合成语音 → audio
    6. 打包回传 Quest

运行:
    pip install fastapi uvicorn httpx
    python middleware.py
    GPT-SoVITS 需单独启动 (端口7860)
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import os as _os
import re
import uuid
from typing import Any

import httpx
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware


def _load_dotenv(path: str = ".env"):
    """加载 .env 文件（简易实现，不依赖 python-dotenv）
    支持通过 MW_ENV 环境变量指定配置文件，如 MW_ENV=.env.ja
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # 优先级: MW_ENV > .env.{lang} > .env
    candidates = []
    env_file = os.environ.get("MW_ENV", "")
    if env_file:
        candidates.append(os.path.join(base_dir, env_file))
    candidates.append(os.path.join(base_dir, ".env"))
    for p in candidates:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, val = line.partition("=")
                        val = val.strip().strip("'\"")
                        os.environ[key.strip()] = val
            print(f"[Config] 加载配置: {p}")
            return


_load_dotenv()
# VRMPresence AstrBot
import presence_service as presence

# ============ 配置 ============
CONFIG = {
    # AstrBot API
    "astrbot_api_base": os.getenv("ASTRBOT_API_BASE", ""),
    "astrbot_api_key": os.getenv("ASTRBOT_API_KEY", ""),
    "astrbot_username": os.getenv("ASTRBOT_USERNAME", ""),
    "astrbot_config_id": os.getenv("ASTRBOT_CONFIG_ID", ""),

    # 本中间件监听
    "host": os.getenv("MW_HOST", "0.0.0.0"),
    "port": int(os.getenv("MW_PORT", "5310")),

    # TTS 配置 - GPT-SoVITS
    "gpt_sovits_api": os.getenv("GPT_SOVITS_API", "http://127.0.0.1:9880"),
    "tts_enabled": os.getenv("TTS_ENABLED", "true").lower() == "true",

    # 参考音频（佳代子日配）
    "ref_audio": os.getenv("GPT_SOVITS_REF_AUDIO", ""),
    "ref_text": os.getenv("GPT_SOVITS_REF_TEXT", ""),
    "ref_lang": os.getenv("GPT_SOVITS_REF_LANG", "ja"),

    # 音频回传方式: "base64" 或 "url"
    "audio_mode": os.getenv("AUDIO_MODE", "base64"),
    # HAST-SEC planner; localhost by default, disable with empty value.
    "hast_plan_url": os.getenv("HAST_PLAN_URL", "http://127.0.0.1:8769/v13-api/plan"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("middleware")

app = FastAPI(title="VRM Quest Middleware")
app.mount("/presence", presence.app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ AstrBot 客户端 ============
class AstrBotClient:
    """封装 AstrBot Open API 调用"""

    def __init__(self, base: str, api_key: str, username: str):
        self.base = base.rstrip("/")
        self.api_key = api_key
        self.username = username
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=120.0)
        return self._client

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def chat(self, message: Any, session_id: str) -> str:
        """发送消息给 AstrBot，返回纯文本回复（处理SSE流式响应）"""
        client = await self._get_client()
        payload = {
            "message": message,
            "session_id": session_id,
            "username": self.username,
            "enable_streaming": False,
            "config_id": CONFIG.get("astrbot_config_id"),
        }
        url = f"{self.base}/chat"
        log.info(f"[AstrBot] POST {url} session={session_id}")
        
        # AstrBot返回SSE格式，逐行解析
        collected_text = ""
        async with client.stream("POST", url, json=payload, headers=self._headers(), timeout=300.0) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                try:
                    data = json.loads(line[6:])
                except json.JSONDecodeError:
                    continue
                
                msg_type = data.get("type", "")
                
                # 收集文本块
                if msg_type == "plain":
                    text = data.get("data", "")
                    if isinstance(text, str) and text:
                        collected_text += text
                
                # 完成消息（非流式模式）
                elif msg_type == "complete":
                    text = data.get("data", "")
                    if isinstance(text, str) and text:
                        collected_text = text  # complete是最终完整文本
                    continue
                
                # 结束
                elif msg_type in ("end", "message_saved", "error"):
                    break
        
        if not collected_text:
            log.warning("[AstrBot] SSE未提取到文本")
            return "（AstrBot回复为空）"
        
        return collected_text

    def _extract_text_from_fallback(self, raw: str) -> str:
        """兜底提取文本"""
        if not raw:
            return "（AstrBot回复为空）"
        return raw

    def _extract_text(self, data: Any) -> str:
        """从 AstrBot 响应里提取纯文本"""
        if isinstance(data, str):
            return data
        if not isinstance(data, dict):
            return str(data)

        # 常见结构: {"status":"ok","data":{"message":[{"type":"plain","text":"..."}]}}
        body = data.get("data", data)

        # 尝试多种路径
        candidates = [
            body.get("message"),
            body.get("messages"),
            body.get("content"),
            body.get("text"),
            body.get("reply"),
            body,
        ]
        for cand in candidates:
            text = self._pick_text(cand)
            if text:
                return text
        return json.dumps(data, ensure_ascii=False)

    def _pick_text(self, node: Any) -> str:
        if node is None:
            return ""
        if isinstance(node, str):
            return node
        if isinstance(node, list):
            parts = []
            for item in node:
                if isinstance(item, dict):
                    if item.get("type") in ("plain", "text") and "text" in item:
                        parts.append(str(item["text"]))
                    elif "text" in item:
                        parts.append(str(item["text"]))
                    elif "content" in item:
                        parts.append(str(item["content"]))
                elif isinstance(item, str):
                    parts.append(item)
            return "".join(parts)
        if isinstance(node, dict):
            for key in ("text", "content", "message", "reply"):
                if key in node and isinstance(node[key], str):
                    return node[key]
        return ""

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None


# ============ 情绪分析 ============
class EmotionAnalyzer:
    """基于关键词+标点的轻量情绪分析"""

    KEYWORDS = {
        "angry": ["生气", "愤怒", "气死", "讨厌", "烦", "滚", "闭嘴", "别闹", "哼"],
        "sorrow": ["难过", "伤心", "哭", "委屈", "遗憾", "唉", "呜呜", "可怜"],
        "surprised": ["哇", "天啊", "居然", "竟然", "不是吧", "真的吗", "！？", "？！", "震惊"],
        "fun": ["哈哈", "嘿嘿", "嘻嘻", "笑死", "有趣", "好玩", "逗"],
        "joy": ["开心", "高兴", "好耶", "太棒", "嘻嘻", "喜欢", "爱", "嘿嘿"],
        "relaxed": ["好的", "嗯嗯", "没问题", "放心", "ok", "行吧"],
    }

    PUNCT_BOOST = {
        "angry": ["！"],
        "surprised": ["！？", "？！", "！！"],
        "joy": ["～", "~"],
    }

    def analyze(self, text: str) -> str:
        if not text:
            return "neutral"

        scores: dict[str, float] = {}
        for emotion, kws in self.KEYWORDS.items():
            score = 0.0
            for kw in kws:
                if kw in text:
                    score += text.count(kw)
            if score > 0:
                scores[emotion] = scores.get(emotion, 0.0) + score

        for emotion, puncts in self.PUNCT_BOOST.items():
            for p in puncts:
                if p in text:
                    scores[emotion] = scores.get(emotion, 0.0) + 0.5

        # 问号多 → surprised
        if text.count("？") + text.count("?") >= 2:
            scores["surprised"] = scores.get("surprised", 0.0) + 0.5

        if not scores:
            return "neutral"
        return max(scores, key=scores.get)


# ============ 运动引擎（HAST-SEC 连续生成） ============
try:
    from motion_engine import MotionEngine
except ImportError:
    MotionEngine = None
_MOTION_MODEL_DIR = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "body_model")
MOTION_ENGINE = None
try:
    if MotionEngine is not None and _os.path.isdir(_MOTION_MODEL_DIR):
        MOTION_ENGINE = MotionEngine(_MOTION_MODEL_DIR)
        log.info("[Motion] 连续动作引擎已加载")
    else:
        log.warning(f"[Motion] 模型目录不存在: {_MOTION_MODEL_DIR}")
except Exception as _e:
    log.warning(f"[Motion] 引擎加载失败: {_e}")

# ============ 技能库（HAST-SEC 导出） ============
_SKILLS_PATH = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "skills_export.json")
SKILLS = {}
try:
    with open(_SKILLS_PATH, "r", encoding="utf-8") as _f:
        SKILLS = json.load(_f)
    log.info(f"[Skills] 加载 {len(SKILLS)} 个技能")
except Exception as _e:
    log.warning(f"[Skills] 加载失败: {_e}")

# ============ 动作生成器 ============
class MotionGenerator:
    """
    根据情绪和文本生成骨骼动作队列。

    输出格式（与 Godot 端 motion_player.gd 匹配）:
    [
      {"t": 0.0, "bones": {"rightUpperArm": {"x": -80}, "head": {"x": 5}}},
      {"t": 0.5, "bones": {...}},
      ...
    ]

    角度单位: 度
    """

    # 各情绪的基础姿态
    EMOTION_POSES = {
        "joy": {
            "head": {"x": -5},
            "spine": {"x": -3},
            "rightUpperArm": {"z": 20},
            "leftUpperArm": {"z": -20},
        },
        "fun": {
            "head": {"x": -10, "y": 8},
            "spine": {"x": -5},
        },
        "angry": {
            "head": {"x": 8},
            "spine": {"x": 5},
            "rightUpperArm": {"y": 30},
            "leftUpperArm": {"y": -30},
        },
        "sorrow": {
            "head": {"x": 20},
            "spine": {"x": 10},
        },
        "surprised": {
            "head": {"x": -12},
            "spine": {"x": -8},
            "rightUpperArm": {"z": 30},
            "leftUpperArm": {"z": -30},
        },
        "relaxed": {
            "head": {"x": 3},
            "spine": {"x": 2},
        },
        "neutral": {
            "head": {"x": 0},
            "spine": {"x": 0},
        },
    }

    def _apply_text_gestures(self, text: str, pose: dict) -> dict:
        """根据文本关键词覆盖动作（只覆盖有明确指令的骨骼，保留情绪姿态）"""
        pose = pose.copy()
        
        t = text
        # 收集关键词触发的骨骼覆盖
        overrides = {}
        if "抬右手" in t:
            overrides["rightUpperArm"] = {"x": -30}
        if "抬左手" in t:
            overrides["leftUpperArm"] = {"x": -30}
        if "举手" in t or "抬手" in t or "双手" in t:
            overrides["rightUpperArm"] = {"x": -30}
            overrides["leftUpperArm"] = {"x": -30}
        if "抬右腿" in t:
            overrides["rightUpperLeg"] = {"x": -20}
        if "抬左腿" in t:
            overrides["leftUpperLeg"] = {"x": -20}
        if "摇头" in t:
            overrides["head"] = {"x": 0, "y": 15}
        if "点头" in t or "好的" in t:
            overrides["head"] = {"x": 10}
        
        # 只覆盖匹配关键词的骨骼，其余保留情绪姿态
        for bone, rot in overrides.items():
            pose[bone] = rot
        return pose

    def generate(self, text: str, emotion: str, est_duration: float = 3.0) -> list:
        """生成动作队列"""
        base_pose = self.EMOTION_POSES.get(emotion, self.EMOTION_POSES["neutral"]).copy()
        
        # 根据文本内容调整动作（关键词检测）
        base_pose = self._apply_text_gestures(text, base_pose)
        
        queue: list[dict] = []

        # 帧1: 起始（idle位置）
        queue.append({"t": 0.0, "bones": {"head": {"x": 0}, "spine": {"x": 0}}})

        # 帧2: 进入情绪姿态
        queue.append({"t": est_duration * 0.15, "bones": base_pose})

        # 帧3: 保持 + 轻微变化（说话时的自然摆动）
        hold_pose = self._vary(base_pose, 0.5)
        queue.append({"t": est_duration * 0.4, "bones": hold_pose})

        # 帧4: 回到另一个变化
        hold_pose2 = self._vary(base_pose, -0.5)
        queue.append({"t": est_duration * 0.7, "bones": hold_pose2})

        # 帧5: 结束回到neutral
        queue.append({"t": est_duration, "bones": self.EMOTION_POSES["neutral"]})

        return queue



    def _vary(self, pose: dict, amount: float) -> dict:
        """对姿态做轻微扰动，避免太僵硬"""
        import random
        result = {}
        for bone, rot in pose.items():
            result[bone] = {}
            for axis, val in rot.items():
                result[bone][axis] = val + random.uniform(-amount, amount) * 2
        return result


# ============ TTS (GPT-SoVITS) ============
class TTSEngine:
    """
    对接 GPT-SoVITS API

    GPT-SoVITS 启动后会暴露 HTTP API (默认 7860)。
    需要先在 GPT-SoVITS WebUI 里:
      1. 设置好参考音频(参考音频路径 + 参考文本)
      2. 或者直接在 config 里填好

    当前实现走 api_v2 接口: POST /tts
    """

    def __init__(self, api_base: str):
        self.api_base = api_base.rstrip("/")
        self.text_lang = "zh"
        self._client: httpx.AsyncClient | None = None

        # GPT-SoVITS 参考音频（佳代子日配）
        self.ref_audio_path: str = CONFIG.get("ref_audio", "")
        self.ref_text: str = CONFIG.get("ref_text", "")
        self.ref_text_lang: str = CONFIG.get("ref_lang", "ja")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=300.0)
        return self._client

    async def synthesize(self, text: str) -> bytes | None:
        """调用 GPT-SoVITS 合成语音，返回 WAV 字节"""
        client = await self._get_client()

        # GPT-SoVITS api_v2 的 /tts 参数
        payload = {
            "text": text,
            "text_lang": self.text_lang,
            "ref_audio_path": self.ref_audio_path,
            "prompt_text": self.ref_text,
            "prompt_lang": self.ref_text_lang,
            "text_split_method": "cut5",
            "batch_size": 1,
            "media_type": "wav",
            "streaming_mode": False,
            # 采样参数（可调）
            "top_k": 15,
            "top_p": 1.0,
            "temperature": 0.9,
            "speed_factor": 1.0,
        }

        # 尝试多个常见端点
        for endpoint in ["/tts", "/api/tts", "/tts/"]:
            url = f"{self.api_base}{endpoint}"
            try:
                log.info(f"[TTS] POST {url}")
                resp = await client.post(url, json=payload)
                if resp.status_code != 200:
                    log.warning(f"[TTS] {url} 返回 {resp.status_code}: {resp.text[:300]}")
                if resp.status_code == 200 and resp.content:
                    ct = resp.headers.get("content-type", "")
                    if "json" in ct:
                        # 可能返回JSON里带base64
                        data = resp.json()
                        audio_b64 = data.get("audio") or data.get("data")
                        if audio_b64:
                            return base64.b64decode(audio_b64)
                    # 直接是音频字节
                    if resp.content[:4] == b"RIFF":
                        return resp.content
                    # 可能是mp3，转wav
                    wav = await self._mp3_to_wav(resp.content)
                    return wav if wav else resp.content
            except httpx.ConnectError:
                log.warning(f"[TTS] 连不上 {url}，GPT-SoVITS 是否启动？")
                return None
            except Exception as e:
                log.warning(f"[TTS] {endpoint} 失败: {e}")
                continue

        log.error("[TTS] 所有端点均失败")
        return None

    async def _mp3_to_wav(self, audio: bytes) -> bytes | None:
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-i", "pipe:0",
                "-f", "wav", "-acodec", "pcm_s16le",
                "-ar", "44100", "-ac", "2",
                "pipe:1",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await proc.communicate(audio)
            return stdout if stdout else None
        except FileNotFoundError:
            return None

    async def set_reference(self, audio_path: str, ref_text: str, lang: str = "zh") -> None:
        """动态设置参考音频"""
        self.ref_audio_path = audio_path
        self.ref_text = ref_text
        self.ref_text_lang = lang

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None


# ============ 中间件核心 ============
# Versioned reply protocol; old Unity clients must be updated before this server.
from skill_protocol import load_quaternion_skills
try:
    QUATERNION_SKILLS = load_quaternion_skills(os.path.dirname(__file__))
except FileNotFoundError:
    QUATERNION_SKILLS = {}

class Middleware:
    def __init__(self):
        self.astrbot = AstrBotClient(
            CONFIG["astrbot_api_base"],
            CONFIG["astrbot_api_key"],
            CONFIG["astrbot_username"],
        )
        self.emotion = EmotionAnalyzer()
        self.motion = MotionGenerator()
        self.tts = TTSEngine(
            CONFIG["gpt_sovits_api"],
        )

    async def _hast_locomotion(self, text: str, session_id: str) -> dict | None:
        """Ask HAST for intent, then expose only its bounded locomotion fields to Unity."""
        url = CONFIG.get("hast_plan_url", "").strip()
        if not url or not text.strip():
            return None
        try:
            session = str(uuid.uuid5(uuid.NAMESPACE_URL, "quest:" + session_id))
            async with httpx.AsyncClient(timeout=12.0) as client:
                response = await client.post(url, json={
                    "text": text[:300], "context": [], "session": session, "generation": "v15"
                })
                response.raise_for_status()
                plan = response.json()
            movement = plan.get("locomotion")
            if isinstance(movement, dict):
                values = {k: float(movement.get(k, 0.0)) for k in ("forward", "right", "turn")}
                stop = bool(movement.get("stop", False))
                if all(__import__("math").isfinite(v) for v in values.values()):
                    if not stop and abs(values["forward"]) < 1e-4 and abs(values["right"]) < 1e-4 and abs(values["turn"]) < 1e-4:
                        return None
                    return {
                        "forward": max(-1.0, min(1.0, values["forward"])),
                        "right": max(-1.0, min(1.0, values["right"])),
                        "turn": max(-90.0, min(90.0, values["turn"])),
                        "stop": stop,
                    }
            return None
        except Exception as exc:
            log.warning("[HAST] locomotion planner unavailable: %s", exc)
            return None

    async def _reply_motion(self, text, emotion, duration, authored=None):
        if text.strip() and os.getenv("HAST_BRIDGE_URL"):
            try:
                async with httpx.AsyncClient(timeout=240.0) as hast:
                    result = await hast.post(os.environ["HAST_BRIDGE_URL"].rstrip("/") + "/clip",
                        json={"text": text, "duration": min(60.0, duration)})
                    result.raise_for_status()
                    clip = result.json()
                if clip.get("motion_mode") != "hast_quaternion_v1" or not isinstance(clip.get("motion"), list) or not clip["motion"]:
                    raise ValueError("Invalid HAST clip")
                return clip["motion"], clip["motion_mode"], max(duration, float(clip["duration"])), None
            except Exception as exc:
                log.exception("[HAST] Reply motion failed")
                return [], "hast_quaternion_v1", duration, str(exc)
        if authored:
            return authored, "source_quaternion_v1", max(duration, float(authored[-1].get("t", 1.0))), None
        return self.motion.generate(text, emotion, duration), "relative", duration, None

    async def handle_text(self, text: str, session_id: str, state=None) -> dict:
        """处理一次文字输入，返回完整响应"""
        # 1. 问 AstrBot
        try:
            reply_text = await self.astrbot.chat(("当前设备状态 " + json.dumps(state, ensure_ascii=False) + "\n用户：" + text) if state else text, session_id)
        except Exception as e:
            log.error(f"[Core] AstrBot 调用失败: {e}")
            reply_text = "（服务器连接失败，我可能掉线了…）"

        # Extract explicit action payload for HAST; keep protocol tags out of speech.
        import re as _move_re
        movement = None
        action_parts = []
        action_pattern = r'<action>\s*(.*?)\s*</action>'
        action_parts = [part.strip() for part in _move_re.findall(action_pattern, reply_text, _move_re.DOTALL) if part.strip()]
        reply_text = _move_re.sub(action_pattern, '', reply_text, flags=_move_re.DOTALL).strip()
        hast_text = ' '.join(action_parts) if action_parts else reply_text
        _match = _move_re.search(r'<move>\s*(.*?)\s*</move>', reply_text, _move_re.DOTALL)
        if _match:
            try:
                _raw = json.loads(_match.group(1))
                import math as _math
                if isinstance(_raw, dict):
                    _values = [float(_raw.get(k, 0)) for k in ('forward', 'right', 'turn')]
                    if all(_math.isfinite(v) for v in _values):
                        movement = dict(zip(('forward', 'right', 'turn'), _values))
                        movement['stop'] = _raw.get('stop') is True
            except (ValueError, TypeError):
                log.warning('[Walk] Invalid move command ignored')
            hast_text = _move_re.sub(r'<move>.*?</move>', '', hast_text, flags=_move_re.DOTALL).strip()

        # HAST is authoritative for natural-language locomotion; legacy <move> remains a fallback.
        if movement is None:
            movement = await self._hast_locomotion(text, session_id)
        log.info("[WalkProtocol] parsed=%s remaining_text_chars=%d", movement, len(hast_text))
        skill_name, protocol_text = self._extract_skill(reply_text)
        reply_text = protocol_text
        log.info(f"[Core] 回复: {reply_text[:120]}")
        motion_data = None
        if skill_name:
            motion_data = self._skill_to_motion(skill_name)
            if motion_data:
                log.info(f"[Core] 技能: {skill_name}, {len(motion_data)}帧")
            else:
                log.warning(f"[Core] 未知技能: {skill_name}")

        # 2. 情绪分析
        emotion = self.emotion.analyze(reply_text)
        log.info(f"[Core] 情绪: {emotion}")

        # 3. TTS 合成
        audio_bytes = None
        duration = max(2.0, len(reply_text) * 0.15)
        if CONFIG["tts_enabled"] and reply_text.strip():
            audio_bytes = await self.tts.synthesize(reply_text)
            if audio_bytes:
                duration = self._estimate_wav_duration(audio_bytes) or duration

        motion_queue, motion_mode, duration, hast_error = await self._reply_motion(
            hast_text, emotion, duration, motion_data)

        # 5. 打包
        log.info("[MotionProtocol] mode=%s frames=%d duration=%.2f", motion_mode, len(motion_queue) if isinstance(motion_queue, list) else 0, duration)
        response: dict[str, Any] = {
            "type": "response",
            "text": reply_text,
            "emotion": emotion,
            "motion": motion_queue,
            "duration": duration,
            "motion_mode": motion_mode,
        }
        if hast_error is not None:
            response["motion_error"] = hast_error
        if movement is not None:
            response["locomotion"] = movement
            log.info("[WalkProtocol] response locomotion=%s", movement)
        if audio_bytes:
            if CONFIG["audio_mode"] == "base64":
                response["audio"] = base64.b64encode(audio_bytes).decode()
                response["audio_format"] = "wav" if audio_bytes[:4] == b"RIFF" else "mp3"
        return response

    async def handle_screenshot(self, image_b64: str, text: str, image_format: str, session_id: str) -> dict:
        """处理截图+文字：上传图片到AstrBot → 让VRM看图回复"""
        try:
            image_bytes = base64.b64decode(image_b64, validate=True)
        except (ValueError, TypeError) as e:
            raise ValueError('截图不是合法的Base64数据') from e
        if not image_bytes or len(image_bytes) > 12 * 1024 * 1024:
            raise ValueError('截图为空或超过12MB限制')
        if image_bytes.startswith(bytes((137, 80, 78, 71, 13, 10, 26, 10))):
            ext = 'png'
        elif image_bytes.startswith(bytes((255, 216, 255))):
            ext = 'jpg'
        else:
            raise ValueError('截图格式不是有效PNG/JPEG')
        log.info(f"[Core] 收到截图: {len(image_bytes)} bytes, {ext}")

        # 1. 上传到AstrBot (不用JSON header，multipart需要浏览器自动设置)
        client = await self.astrbot._get_client()
        url = f"{self.astrbot.base}/file"
        files = {"file": (f"screenshot.{ext}", image_bytes, f"image/{ext}")}
        resp = await client.post(url, files=files, headers={"Authorization": f"Bearer {self.astrbot.api_key}"})
        resp.raise_for_status()
        data = resp.json()
        attachment_id = data.get("data", {}).get("id", "") or data.get("data", {}).get("attachment_id", "")
        if not attachment_id:
            raise Exception(f"上传失败: {data}")
        log.info(f"[Core] 图片已上传, id={attachment_id}")

        # 2. 用消息parts格式发送给AstrBot
        response = await self._ask_astrbot_with_image(attachment_id, text, session_id)
        return response

    async def _ask_astrbot_with_image(self, attachment_id: str, text: str, session_id: str) -> dict:
        """带图片问AstrBot"""
        client = await self.astrbot._get_client()
        payload = {
            "message": [
                {"type": "image", "attachment_id": attachment_id},
                {"type": "plain", "text": text or "看看这个"},
            ],
            "session_id": session_id,
            "username": self.astrbot.username,
            "enable_streaming": False,
            "config_id": CONFIG.get("astrbot_config_id"),
        }
        url = f"{self.astrbot.base}/chat"
        log.info(f"[Core] 带图问AstrBot, attachment={attachment_id}, text={text[:50]}")

        collected_text = ""
        async with client.stream("POST", url, json=payload, headers=self.astrbot._headers(), timeout=300.0) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                try:
                    d = json.loads(line[6:])
                except json.JSONDecodeError:
                    continue
                t = d.get("type", "")
                if t == "plain":
                    txt = d.get("data", "")
                    if isinstance(txt, str):
                        collected_text += txt
                elif t == "complete":
                    txt = d.get("data", "")
                    if isinstance(txt, str) and txt:
                        collected_text = txt
                    break
                elif t in ("end", "message_saved", "error"):
                    break

        if not collected_text:
            collected_text = "（看图没看明白…）"
        log.info(f"[Core] 回复(看图): {collected_text[:120]}")

        # 后续：情绪、TTS、动作
        emotion = self.emotion.analyze(collected_text)
        audio_bytes = None
        duration = max(2.0, len(collected_text) * 0.15)
        if CONFIG["tts_enabled"]:
            audio_bytes = await self.tts.synthesize(collected_text)
            if audio_bytes:
                duration = self._estimate_wav_duration(audio_bytes) or duration
        motion_queue, motion_mode, duration, hast_error = await self._reply_motion(collected_text, emotion, duration)

        response: dict[str, Any] = {
            "type": "response",
            "text": collected_text,
            "motion_mode": motion_mode,
            "motion_error": hast_error,
            "emotion": emotion,
            "motion": motion_queue,
            "duration": duration,
        }
        if audio_bytes and CONFIG["audio_mode"] == "base64":
            response["audio"] = base64.b64encode(audio_bytes).decode()
            response["audio_format"] = "wav" if audio_bytes[:4] == b"RIFF" else "mp3"
        return response

    def _extract_motion(self, text: str) -> tuple:
        """从LLM回复中提取 <motion>[...]</motion> JSON块，返回(motion_list, cleaned_text)"""
        import re
        pattern = r'<motion>\s*(.*?)\s*</motion>'
        match = re.search(pattern, text, re.DOTALL)
        if not match:
            return None, self._strip_tags(text)
        raw_json = match.group(1)
        cleaned = text[:match.start()].strip()
        cleaned = self._strip_tags(cleaned)
        try:
            motion_list = json.loads(raw_json)
            if isinstance(motion_list, list) and len(motion_list) > 0:
                return motion_list, cleaned
        except json.JSONDecodeError as e:
            log.warning(f"[Core] motion JSON解析失败: {e}")
        return None, cleaned

    def _extract_skill(self, text: str) -> tuple:
        """从LLM回复中提取技能名，并从文本中删除所有技能名和标签"""
        import re
        # 1. 先匹配标签内的技能名
        matches = re.findall(r'<skill>\s*(.*?)\s*</skill>', text, re.DOTALL)
        name = matches[0].strip() if matches else None
        # 2. 删除所有<skill>...</skill>标签（含未闭合）
        cleaned = re.sub(r'<skill>.*?(?:</skill>|$)', '', text, flags=re.DOTALL)
        # 3. 删除残留的技能名（如果没标签）
        if SKILLS:
            for sk in sorted(SKILLS.keys(), key=len, reverse=True):
                if sk in cleaned:
                    if name is None:
                        name = sk
                    cleaned = cleaned.replace(sk, '')
        cleaned = self._strip_tags(cleaned)
        return name, cleaned

    def _skill_to_motion(self, name: str) -> list | None:
        """技能名 → motion 帧序列"""
        if not name or name not in SKILLS:
            return None
        return QUATERNION_SKILLS.get(name)

    def _strip_tags(self, text: str) -> str:
        """清除 <tool_code>、<tool_use> 等所有XML标签，避免污染TTS文本"""
        import re
        text = re.sub(r'<[^>]+>', '', text)
        return text.strip()

    def _estimate_wav_duration(self, data: bytes) -> float | None:
        """从 WAV 头估算时长"""
        if len(data) < 44 or data[:4] != b"RIFF":
            return None
        try:
            import struct
            # 找 data chunk
            pos = 12
            byte_rate = 176400  # 默认 44100*2*2
            data_size = 0
            while pos < len(data) - 8:
                chunk_id = data[pos:pos+4]
                chunk_size = struct.unpack("<I", data[pos+4:pos+8])[0]
                if chunk_id == b"fmt ":
                    byte_rate = struct.unpack("<I", data[pos+16:pos+20])[0]
                elif chunk_id == b"data":
                    data_size = chunk_size
                    break
                pos += 8 + chunk_size
                if chunk_size % 2 == 1: pos += 1  # WAV chunk word alignment
            if byte_rate > 0 and data_size > 0:
                return data_size / byte_rate
        except Exception:
            pass
        return None

    async def close(self):
        await self.astrbot.close()
        await self.tts.close()


import astr_admin
astr_admin.load(CONFIG)
mw = Middleware()
astr_admin.attach(app, CONFIG, mw.astrbot)
presence.astr_client = mw.astrbot


# ============ WebSocket 端点 ============
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_addr = websocket.client.host if websocket.client else "?"
    log.info(f"[WS] Quest 客户端连接: {client_addr}")

    session_id = f"quest3_{client_addr}"

    # 后台推送任务：每50ms生成一帧连续动作
    push_task = None
    if MOTION_ENGINE is not None:
        import asyncio as _asyncio
        async def _push_loop():
            engine = MotionEngine(_MOTION_MODEL_DIR)  # 每个连接独立实例
            try:
                while True:
                    frame = engine.frame(0.05)
                    await websocket.send_text(json.dumps({
                        "type": "frame",
                        "bones": frame["bones"],
                        "jaw": frame["jaw"],
                        "face": frame["face"],
                    }, ensure_ascii=False))
                    await _asyncio.sleep(0.05)
            except Exception as e:
                log.info(f"[WS] 推送结束: {e}")
        push_task = _asyncio.create_task(_push_loop())
        log.info("[WS] 连续动作推送已启动")

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning(f"[WS] 非法JSON: {raw[:100]}")
                continue

            msg_type = msg.get("type", "")
            log.info(f"[WS] 收到: type={msg_type}")

            state = presence.current(str(msg.get("presence_id", "")))
            if msg_type == "input":
                text = msg.get("text", "")
                if not text:
                    continue
                response = await mw.handle_text(text, session_id, state)
                log.info(f"[WS] 发送响应: text={response.get('text','')[:30]}, audio={'有' if response.get('audio') else '无'}")
                response["turn_id"] = msg.get("turn_id", "")
                await websocket.send_text(json.dumps(response, ensure_ascii=False))

            elif msg_type == "voice_input":
                # 语音输入：base64 音频 → 转文字 → 走文字流程
                audio_b64 = msg.get("audio", "")
                text = await transcribe_audio(audio_b64)
                if text:
                    await websocket.send_text(json.dumps({
                        "type": "stt_result", "text": text
                    }, ensure_ascii=False))
                    response = await mw.handle_text(text, session_id, state)
                    response["turn_id"] = msg.get("turn_id", "")
                    await websocket.send_text(json.dumps(response, ensure_ascii=False))
                else:
                    await websocket.send_text(json.dumps({
                        "type": "error", "message": "语音识别失败"
                    }, ensure_ascii=False))

            elif msg_type == "screenshot":
                image_b64 = msg.get("image", "")
                text = msg.get("text", "")
                audio_b64 = msg.get("audio", "")  # 可选语音

                if audio_b64:
                    # 有语音 → 先转文字
                    try:
                        text = await transcribe_audio(audio_b64)
                        if not text:
                            text = "看看这个"
                    except Exception as e:
                        log.warning(f"[WS] 语音转写失败: {e}")
                        text = "看看这个"

                if not isinstance(image_b64, str) or not image_b64:
                    await websocket.send_text(json.dumps({
                        "type": "error", "message": "没有图片数据"
                    }, ensure_ascii=False))
                    continue

                try:
                    response = await mw.handle_screenshot(image_b64, text, msg.get("image_format", "png"), session_id)
                    response["turn_id"] = msg.get("turn_id", "")
                    await websocket.send_text(json.dumps(response, ensure_ascii=False))
                except Exception as e:
                    log.error(f"[WS] 截图处理失败: {e}", exc_info=True)
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"看图时出错了: {str(e)[:80]}"
                    }, ensure_ascii=False))

            elif msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))

            else:
                log.warning(f"[WS] 未知类型: {msg_type}")

    except WebSocketDisconnect:
        if push_task is not None:
            push_task.cancel()
        log.info(f"[WS] 客户端断开: {client_addr}")
    except Exception as e:
        log.error(f"[WS] 异常: {e}", exc_info=True)


async def transcribe_audio(audio_b64: str) -> str | None:
    """语音转文字。默认用 AstrBot 的 STT，或本地 whisper。"""
    # 方案A: 转发给 AstrBot 的语音识别接口（如果有）
    # 方案B: 本地 faster-whisper
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        log.warning("[STT] faster-whisper 未安装")
        return None

    try:
        audio_bytes = base64.b64decode(audio_b64)
        # 写入临时文件
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
            f.write(audio_bytes)
            tmp_path = f.name

        model = _get_whisper_model()
        segments, _ = model.transcribe(tmp_path, language="zh")
        text = "".join(seg.text for seg in segments).strip()
        os.unlink(tmp_path)
        log.info(f"[STT] 识别: {text}")
        return text
    except Exception as e:
        log.error(f"[STT] 失败: {e}")
        return None


_whisper_model = None

def _get_whisper_model():
    global _whisper_model
    if _whisper_model is None:
        from faster_whisper import WhisperModel
        _whisper_model = WhisperModel("small", device="auto", compute_type="int8")
    return _whisper_model


# ============ HTTP 端点（调试用） ============
@app.get("/")
async def root():
    return {
        "service": "VRM Quest Middleware",
        "status": "ok",
        "astrbot": CONFIG["astrbot_api_base"],
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/test")
async def test_chat(payload: dict):
    """HTTP测试接口，方便先用 curl/postman 验证"""
    text = payload.get("text", "你好")
    session_id = payload.get("session_id", "test_user")
    return await mw.handle_text(text, session_id)


@app.on_event("shutdown")
async def shutdown():
    await mw.close()


if __name__ == "__main__":
    import uvicorn
    log.info("=" * 50)
    log.info("VRM Quest Middleware 启动")
    log.info(f"  AstrBot: {CONFIG['astrbot_api_base']}")
    log.info(f"  监听: {CONFIG['host']}:{CONFIG['port']}")
    log.info(f"  TTS: GPT-SoVITS @ {CONFIG['gpt_sovits_api']}")
    log.info("=" * 50)
    uvicorn.run(app, host=CONFIG["host"], port=CONFIG["port"])
