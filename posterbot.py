import asyncio
import aiohttp
import itertools
import os
from pyrogram import Client, filters
from pyrogram.types import Message
from motor.motor_asyncio import AsyncIOMotorClient

# ==========================================
# 🤖 CREDENTIALS & DB
# ==========================================
API_ID = 33675350
API_HASH = "2f97c845b067a750c9f36fec497acf97"

# Pehle token ko hum "Main Control Bot" bana rahe hain
MAIN_BOT_TOKEN = "8795437034:AAHR9ZfmuQT2cQPfkgaKS6NYH9N6owLNzP0"
MONGO_URL = "mongodb+srv://salonisingh6265_db_user:U50ONNZZFUbh0iQI@cluster0.41mb27f.mongodb.net/?appName=Cluster0"

# ⚠️ YAHAN APNA DUMP CHANNEL ID DAALNA MAT BHOOLNA
DUMP_CHAT_ID = "-100123456789"  

# Baaki 4 bots dump karne ka kaam karenge (Main bot ko bhi daal sakte ho rotate me)
TOKENS = [
    "8224617875:AAH_ltijOHgJoGrKeU_Hge6jM_waL4ppvGw",
    "8698094569:AAGPmLkqOkuY2k1cBl0py__mVZGZH9SnHhk",
    "8685863099:AAGLdE6-0MnuL3m73rROKkmzp4geCokMgJM",
    "8674720144:AAFQQx8AUNtO57ucHVt8JKIhmVxPbxvuvyY"
]

PROXIES = [
    "http://hUNRBWVpH:FJ45J8z5F@84.246.81.238:64198/",
    "http://F5MyYJBG2:s4m1uMNCV@170.168.179.182:64252/",
    "http://5EeYxYhHn:DhcBKnYSq@103.152.16.238:63232/",
    "http://qD24wiqbY:FutFaxTpW@45.140.173.176:64752/",
    "http://QkM6tcTaC:Yk1Yf6Y1D@83.138.51.15:64344/"
]

token_pool = itertools.cycle(TOKENS)
proxy_pool = itertools.cycle(PROXIES)

# Init Pyrogram Client
app = Client("poster_archiver", api_id=API_ID, api_hash=API_HASH, bot_token=MAIN_BOT_TOKEN)

# Init DB
mongo = AsyncIOMotorClient(MONGO_URL)
poster_cache = mongo["HanimeArchiveDB"]["PosterCache_VPS"]

# ==========================================
# 📊 STATS & TRACKING
# ==========================================
STATS = {
    "running": False,
    "success": 0,
    "skipped": 0,
    "failed": 0
}

def save_failed(slug):
    """Failed slugs ko f.txt me save karne ka function."""
    with open("f.txt", "a") as f:
        f.write(slug + "\n")
    STATS["failed"] += 1

# ==========================================
# 🕸️ HELPERS: DOWNLOAD & UPLOAD
# ==========================================
async def fetch_hanime_api(slug, session):
    url = f"https://hw.hanime.tv/api/v8/video?id={slug}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://hanime.tv/"}
    proxy = next(proxy_pool)
    
    try:
        async with session.get(url, headers=headers, proxy=proxy, timeout=15) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        print(f"⚠️ API Fetch Error ({slug}): {e}")
    return None

async def download_image(url, session):
    proxy = next(proxy_pool)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://hanime.tv/"}
    try:
        async with session.get(url, headers=headers, proxy=proxy, timeout=15) as resp:
            if resp.status == 200:
                return await resp.read()
    except Exception as e:
        print(f"⚠️ DL Error ({url}): {e}")
    return None

async def upload_to_telegram(image_bytes, caption, session):
    token = next(token_pool)
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    
    data = aiohttp.FormData()
    data.add_field('chat_id', DUMP_CHAT_ID)
    data.add_field('photo', image_bytes, filename='cover.jpg', content_type='image/jpeg')
    data.add_field('caption', caption)
    
    try:
        async with session.post(url, data=data) as resp:
            res = await resp.json()
            if res.get("ok"):
                return res["result"]["message_id"]
            elif res.get("error_code") == 429:
                await asyncio.sleep(res["parameters"]["retry_after"])
                return await upload_to_telegram(image_bytes, caption, session)
    except Exception as e:
        print(f"⚠️ TG Request Error: {e}")
    return None

# ==========================================
# 🚀 CORE WORKER LOGIC
# ==========================================
async def process_slug(main_slug, session):
    if not STATS["running"]: return

    data = await fetch_hanime_api(main_slug, session)
    if not data or 'hentai_video' not in data:
        print(f"❌ Failed to get main data for {main_slug}")
        save_failed(main_slug)
        return

    episodes = data.get('hentai_franchise', {}).get('hentai_videos', [])
    if not episodes:
        episodes = [{"slug": data['hentai_video']['slug'], "name": data['hentai_video']['name']}]

    for ep in episodes:
        if not STATS["running"]: break

        ep_slug = ep['slug']
        exists = await poster_cache.find_one({"slug": ep_slug})
        if exists:
            STATS["skipped"] += 1
            continue
            
        ep_data = await fetch_hanime_api(ep_slug, session)
        if not ep_data:
            save_failed(ep_slug)
            continue
        
        v_data = ep_data['hentai_video']
        cover_url = v_data.get('cover_url') or v_data.get('poster_url')
        title = v_data.get('name', ep_slug)
        
        if not cover_url:
            save_failed(ep_slug)
            continue
            
        img_bytes = await download_image(cover_url, session)
        if not img_bytes:
            save_failed(ep_slug)
            continue
            
        caption = f"🎬 **{title}**\n🧬 Slug: `{ep_slug}`"
        msg_id = await upload_to_telegram(img_bytes, caption, session)
        
        if msg_id:
            await poster_cache.insert_one({"slug": ep_slug, "title": title, "cover_msg_id": msg_id, "cover_url": cover_url})
            STATS["success"] += 1
            print(f"✅ Dumped: {ep_slug}")
        else:
            save_failed(ep_slug)
            
        del img_bytes 
        await asyncio.sleep(1.5)

async def run_archiver(message):
    STATS["running"] = True
    STATS["success"] = 0
    STATS["skipped"] = 0
    STATS["failed"] = 0
    
    # Naya run hone par f.txt purana wala delete kardo
    if os.path.exists("f.txt"):
        os.remove("f.txt")

    try:
        with open("slugs.txt", "r") as f:
            slugs = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        STATS["running"] = False
        return await message.reply("❌ `slugs.txt` file nahi mili!")

    await message.reply(f"🚀 **Dumper Started!**\nFound {len(slugs)} franchises in file.")

    async with aiohttp.ClientSession() as session:
        for slug in slugs:
            if not STATS["running"]: break
            await process_slug(slug, session)

    STATS["running"] = False
    await message.reply("🛑 **Dumper Cycle Completed!**\nUse `/status` to see results.")

# ==========================================
# 💬 TELEGRAM COMMANDS
# ==========================================
@app.on_message(filters.command("start_dump"))
async def start_cmd(client, message: Message):
    if STATS["running"]:
        return await message.reply("⚠️ Dumper is already running!")
    asyncio.create_task(run_archiver(message))

@app.on_message(filters.command("stop_dump"))
async def stop_cmd(client, message: Message):
    if not STATS["running"]:
        return await message.reply("⚠️ Dumper is not running.")
    STATS["running"] = False
    await message.reply("🛑 Stopping Dumper... (will stop after current episode completes).")

@app.on_message(filters.command("status"))
async def status_cmd(client, message: Message):
    state = "🟢 RUNNING" if STATS["running"] else "🔴 STOPPED"
    
    text = (
        f"📊 **Poster Dumper Status:**\n\n"
        f"⚙️ **Engine:** {state}\n"
        f"✅ **Success (Dumped):** {STATS['success']}\n"
        f"⏭️ **Skipped (Already in DB):** {STATS['skipped']}\n"
        f"❌ **Failed:** {STATS['failed']}\n"
    )
    
    if STATS["failed"] > 0 and os.path.exists("f.txt"):
        await message.reply_document(document="f.txt", caption=text)
    else:
        await message.reply_text(text)

if __name__ == "__main__":
    print("🤖 Master Poster Archiver Bot Started!")
    app.run()
