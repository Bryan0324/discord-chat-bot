import asyncio
import logging
import time
import discord
from discord.ext import commands
import discord.ext.voice_recv as voice_recv

from .rooms import Message, Room, RoomRegistry, User

logger = logging.getLogger(__name__)

async def dummy_asr_function(user_id: int, pcm_data: bytes) -> str:
    return "text"

class ASR_Sink(voice_recv.AudioSink):
    def __init__(
        self,
        bot: commands.Bot,
        voice_channel: discord.VoiceChannel,
        room_registry: RoomRegistry | None = None,
    ):
        super().__init__()
        self.voice_channel = voice_channel
        self.user_buffers = {}
        self.last_spoken = {}
        self.bot = bot
        self.room_registry = room_registry
        self.closed = False
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
            text_result = await dummy_asr_function(user_id, pcm_data)
            
            if text_result:
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
        except Exception as e:
            logger.error("處理 ASR 或發送訊息時發生錯誤: %s", e)

    def cleanup(self):
        self.closed = True
        self.user_buffers.clear()
        self.last_spoken.clear()
