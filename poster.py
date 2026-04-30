import asyncio
import aiohttp
import io
import os
import itertools
from pyrogram import Client, filters
from pyrogram.types import Message
from motor.motor_asyncio import AsyncIOMotorClient

# ==========================================
# 🤖 BOT CREDENTIALS & DB
# ==========================================
API_ID = 33675350
API_HASH = "2f97c845b067a750c9f36fec497acf97"

MAIN_BOT_TOKEN = "8795437034:AAHR9ZfmuQT2cQPfkgaKS6NYH9N6owLNzP0"

TOKENS = [
    "8795437034:AAHR9ZfmuQT2cQPfkgaKS6NYH9N6owLNzP0", 
    "8224617875:AAH_ltijOHgJoGrKeU_Hge6jM_waL4ppvGw",
    "8698094569:AAGPmLkqOkuY2k1cBl0py__mVZGZH9SnHhk",
    "8685863099:AAGLdE6-0MnuL3m73rROKkmzp4geCokMgJM",
    "8674720144:AAFQQx8AUNtO57ucHVt8JKIhmVxPbxvuvyY"
]

MONGO_URL = "mongodb+srv://salonisingh6265_db_user:U50ONNZZFUbh0iQI@cluster0.41mb27f.mongodb.net/?appName=Cluster0"
DUMP_CHAT_ID = -1003831827071

token_pool = itertools.cycle(TOKENS)

app = Client("poster_archiver", api_id=API_ID, api_hash=API_HASH, bot_token=MAIN_BOT_TOKEN)
mongo = AsyncIOMotorClient(MONGO_URL)
poster_cache = mongo["HanimeArchiveDB"]["PosterCache_VPS"]

STATS = {"running": False, "success": 0, "skipped": 0, "failed": 0}

def save_failed(slug, reason="Unknown"):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    f_path = os.path.join(script_dir, "f.txt")
    with open(f_path, "a") as f:
        f.write(f"{slug} - {reason}\n")
    STATS["failed"] += 1

# ==========================================
# 🕸️ SMART API & FLOOD-PROOF DOWNLOADS
# ==========================================
async def fetch_api_smart(base_slug, session):
    variations = [base_slug, f"{base_slug}-1", f"{base_slug}-episode-1"]
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for slug_to_try in variations:
        url = f"https://hanime.tv/api/v8/video?id={slug_to_try}"
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if 'hentai_video' in data: return data
        except Exception: pass
    return None

async def download_image(url, session):
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://hanime.tv/"}
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200: 
                return await resp.read()
    except Exception as e:
        print(f"DL Error: {e}")
    return None

async def upload_to_telegram_raw(image_bytes, caption, session, attempt=1):
    if attempt > 5:
        print("⚠️ Max retries reached! Telegram is rejecting uploads.")
        return None

    token = next(token_pool)
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    
    photo_io = io.BytesIO(image_bytes)
    
    data = aiohttp.FormData()
    data.add_field('chat_id', str(DUMP_CHAT_ID))
    data.add_field('photo', photo_io, filename='cover.jpg', content_type='image/jpeg')
    data.add_field('caption', caption)
    
    try:
        async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            res = await resp.json()
            if res.get("ok"):
                return res["result"]["message_id"]
            elif res.get("error_code") == 429:
                wait_time = res["parameters"].get("retry_after", 0)
                print(f"🚨 FLOODWAIT DETECTED! Telegram asked to wait {wait_time}s.")
                print(f"💤 Sleeping for 2 HOURS (7200 seconds) to keep bots safe...")
                
                # Smart Sleep: 2 hours ka loop jo /stop_dump se break ho sakta hai
                for _ in range(7200):
                    if not STATS["running"]:
                        print("🛑 Engine stopped during 2-hour sleep!")
                        return None
                    await asyncio.sleep(1)
                    
                print("🔄 2 Hours completed! Waking up and resuming uploads...")
                return await upload_to_telegram_raw(image_bytes, caption, session, attempt + 1)
            else:
                print(f"⚠️ TG Error on Token {token[:10]}: {res}")
    except asyncio.TimeoutError:
        print(f"⚠️ TG Timeout (Network slow)! Retrying with next bot...")
        return await upload_to_telegram_raw(image_bytes, caption, session, attempt + 1)
    except Exception as e:
        print(f"⚠️ TG Upload Crash: {e}")
    return None

# ==========================================
# 🚀 CORE LOGIC
# ==========================================
async def process_slug(main_slug, session, manual=False):
    if not STATS["running"] and not manual: return
    
    print(f"\n🔍 Searching API for: {main_slug}")
    data = await fetch_api_smart(main_slug, session)
    
    if not data:
        print(f"❌ Failed: {main_slug} not found on API")
        save_failed(main_slug, "API_Failed_404")
        return

    episodes = []
    if 'hentai_franchise_hentai_videos' in data and data['hentai_franchise_hentai_videos']:
        episodes = data['hentai_franchise_hentai_videos']
    elif 'hentai_video' in data:
        episodes = [data['hentai_video']]
        
    print(f"📚 Found {len(episodes)} episodes for {main_slug}!")

    for ep in episodes:
        if not STATS["running"] and not manual: break
        
        ep_slug = ep.get('slug')
        title = ep.get('name', ep_slug)
        cover_url = ep.get('cover_url') or ep.get('poster_url')
        
        print(f"  👉 Processing: {ep_slug}")
        
        if not ep_slug or not cover_url:
            save_failed(ep_slug or main_slug, "Missing_Image_URL")
            continue
            
        if await poster_cache.find_one({"slug": ep_slug}):
            if not manual: STATS["skipped"] += 1
            print(f"  ⏭️ Skipped {ep_slug} (Already in DB!)")
            continue
            
        img_bytes = await download_image(cover_url, session)
        if not img_bytes:
            print(f"  ❌ Image download failed for {ep_slug}")
            save_failed(ep_slug, "Image_DL_Failed")
            continue
            
        caption = f"🎬 **{title}**\n🧬 Slug: `{ep_slug}`\n⚡ Powered by **AyuPrime**"
        
        msg_id = await upload_to_telegram_raw(img_bytes, caption, session)
        
        if msg_id:
            await poster_cache.insert_one({
                "slug": ep_slug, 
                "title": title, 
                "cover_msg_id": msg_id, 
                "cover_url": cover_url
            })
            if not manual: STATS["success"] += 1
            print(f"  ✅ Uploaded & Saved: {ep_slug}")
            
            # 🕒 EXACTLY 5 SECONDS GAP BETWEEN UPLOADS
            await asyncio.sleep(5)
        else:
            save_failed(ep_slug, "TG_Upload_Error")
            
        del img_bytes 

async def run_archiver(message):
    STATS["running"] = True
    STATS["success"], STATS["skipped"], STATS["failed"] = 0, 0, 0
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    slugs_path = os.path.join(script_dir, "slugs.txt")
    f_path = os.path.join(script_dir, "f.txt")

    if os.path.exists(f_path): os.remove(f_path)

    try:
        with open(slugs_path, "r") as f:
            slugs = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        STATS["running"] = False
        return await message.reply(f"❌ `{slugs_path}` nahi mili!")

    await message.reply(f"🚀 **AyuPrime Dumper Started!** (Strict Delay Engine)\nFound {len(slugs)} items. 🔥")

    conn = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=conn) as session:
        for slug in slugs:
            if not STATS["running"]: break
            await process_slug(slug, session)

    STATS["running"] = False
    await message.reply("🛑 **Cycle Completed!** Use `/status` to see final report.")

# ==========================================
# 💬 COMMANDS & FAST PING LOGIC
# ==========================================
@app.on_message(filters.command("start_dump"))
async def start_cmd(client, message):
    if STATS["running"]: return await message.reply("⚠️ Already running!")
    asyncio.create_task(run_archiver(message))

@app.on_message(filters.command("add"))
async def add_cmd(client, message: Message):
    if len(message.command) < 2:
        return await message.reply("⚠️ Syntax: `/add natsu-to-hako`")
    
    raw_query = " ".join(message.command[1:]).strip()
    slug = raw_query.replace('https://hanime.tv/videos/hentai/', '').strip('/').split('?')[0].lower()
    
    msg = await message.reply(f"🔍 Manually scanning `{slug}`...")
    
    async with aiohttp.ClientSession() as session:
        await process_slug(slug, session, manual=True)
        
    await msg.edit_text(f"✅ Manual scan for `{slug}` completed! Check Terminal for logs.")

async def wake_up_bots_background():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i, token in enumerate(TOKENS):
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {"chat_id": str(DUMP_CHAT_ID), "text": f"✅ Bot {i+1} is Active & Ready!"}
            tasks.append(session.post(url, json=payload, timeout=5))
        await asyncio.gather(*tasks, return_exceptions=True)

@app.on_message(filters.command("status"))
async def status_cmd(client, message):
    asyncio.create_task(wake_up_bots_background())
    
    state = "🟢 RUNNING" if STATS["running"] else "🔴 STOPPED"
    text = (
        f"📊 **Dumper Status:**\n\n"
        f"⚙️ Engine: {state}\n"
        f"✅ Success: {STATS['success']}\n"
        f"⏭️ Skipped: {STATS['skipped']}\n"
        f"❌ Failed: {STATS['failed']}"
    )
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    f_path = os.path.join(script_dir, "f.txt")
    
    if STATS["failed"] > 0 and os.path.exists(f_path):
        await message.reply_document(document=f_path, caption=text)
    else:
        await message.reply_text(text)

@app.on_message(filters.command("stop_dump"))
async def stop_cmd(client, message):
    STATS["running"] = False
    await message.reply("🛑 Stopping engine...")

if __name__ == "__main__":
    print("🚀 AyuPrime Ultimate Archiver Started (Strict Anti-Flood)!")
    app.run()