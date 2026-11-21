import asyncio
import logging
import time
import discord
from discord.ext import commands
import discord.ext.voice_recv as voice_recv

from .types import ChatBot, Message, Room, RoomRegistry, User

logger = logging.getLogger(__name__)

class ASR_Sink(voice_recv.AudioSink):
    def __init__(
        self,
        bot: ChatBot,
        voice_channel: discord.VoiceChannel,
        room_registry: RoomRegistry | None = None,
        asr_manager=None,
        departure_channel=None,
        idle_timeout_seconds: float = 300,
    ):
        super().__init__()
        self.voice_channel = voice_channel
        self.user_buffers = {}
        self.last_spoken = {}
        self.bot = bot
        self.room_registry = room_registry
        self.asr_manager = asr_manager
        self.departure_channel = departure_channel
        self.idle_timeout_seconds = idle_timeout_seconds
        self.closed = False
        self.last_activity = time.monotonic()
        self.departure_started = False
        self.bot_loop = asyncio.get_event_loop()
        self.bot_loop.create_task(self.check_silence_loop())

    def wants_opus(self) -> bool:
        return False

    def is_closed(self) -> bool:
        return self.closed

    def write(self, user: discord.User, data: voice_recv.VoiceData):
        if user is None:
            return

        user_id = user.id
        self.last_activity = time.monotonic()
        if user_id not in self.user_buffers:
            self.user_buffers[user_id] = bytearray()
            
        pcm = getattr(data, "pcm", None)
        if pcm:
            self.user_buffers[user_id].extend(pcm)
        self.last_spoken[user_id] = time.monotonic()

    async def check_silence_loop(self):
        while not self.is_closed():
            await asyncio.sleep(0.5)
            now = time.monotonic()

            if (
                not self.departure_started
                and now - self.last_activity >= self.idle_timeout_seconds
            ):
                self.bot_loop.create_task(self.leave_after_idle())
            
            for user_id in list(self.last_spoken.keys()):
                if now - self.last_spoken[user_id] > 1.5:
                    pcm_to_process = bytes(self.user_buffers[user_id])
                    
                    del self.user_buffers[user_id]
                    del self.last_spoken[user_id]
                    
                    if len(pcm_to_process) > 0:
                        logger.info(
                            "Voice silence detected: user_id=%s, pcm_bytes=%d",
                            user_id,
                            len(pcm_to_process),
                        )
                        self.bot_loop.create_task(
                            self.process_and_send(user_id, pcm_to_process)
                        )

    async def process_and_send(self, user_id: int, pcm_data: bytes):
        try:
            if self.asr_manager is None:
                logger.warning("未設定 ASR provider，跳過語音辨識")
                return
            logger.info("開始辨識聲音")
            asr_result = await self.asr_manager.transcribe(pcm_data)
            text_result = asr_result.text
            
            if text_result:
                logger.info(f"recognized: {text_result}")
                user = self.bot.get_user(user_id)
                if user is None:
                    user = await self.bot.fetch_user(user_id)
                message = Message.voice(
                    author=User.from_discord(user),
                    room=Room.from_discord(
                        self.voice_channel,
                        registry=self.room_registry,
                    ),
                    text=text_result,
                    pcm_data=pcm_data,
                )
                await self.bot.sayuki.process_chat_message(message)
            else:
                logger.warning(f"recognized None")
        except Exception as e:
            logger.error("處理 ASR 或發送訊息時發生錯誤: %s", e)

    async def send_departure_message(self, text: str) -> None:
        channel = self.voice_channel
        try:
            await channel.send(text)
        except Exception as exc:
            logger.warning("傳送語音離開通知失敗: %s", exc)

    async def speak_departure_message(self, text: str) -> None:
        """Reserved interface for future text-to-speech departure announcements."""
        return

    async def leave_after_idle(self) -> None:
        if self.departure_started:
            return
        self.departure_started = True
        message = "已經 5 分鐘沒有人說話了，我先離開語音頻道囉～"

        await self.send_departure_message(message)
        await self.speak_departure_message(message)

        try:
            voice_client = self.voice_channel.guild.voice_client
            if voice_client is not None:
                voice_client.stop_listening()
        except Exception as exc:
            logger.warning("停止語音接收失敗: %s", exc)

        try:
            voice_client = self.voice_channel.guild.voice_client
            if voice_client is not None:
                await voice_client.disconnect()
        except Exception as exc:
            logger.warning("離開語音頻道失敗: %s", exc)
        finally:
            self.cleanup()

    def cleanup(self):
        self.closed = True
        self.user_buffers.clear()
        self.last_spoken.clear()
