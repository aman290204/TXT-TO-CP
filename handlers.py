
import os
import re
import time
import asyncio
import json
import uuid
import requests
import cloudscraper
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, MessageNotModified
from vars import *
from ath import *
import thanos as helper
from db import db

import gdrive

# Worker task function
async def process_link_task(queue, links, raw_text2, raw_text3, raw_text4, b_name, PRENAME, CR, bot, m, channel_id, watermark, thumb, cc, cc1, cchtml, ccimg, cczip, stats, drive_service, drive_folder_id):
    while True:
        try:
            index = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
            
        try:
            # Stats tracking
            # stats is a dict: {'count': int, 'failed': int, 'video': int, ...}
            
            i = index
            Vxy = links[i][1].replace("file/d/","uc?export=download&id=").replace("www.youtube-nocookie.com/embed", "youtu.be").replace("?modestbranding=1", "").replace("/view?usp=sharing","")
            url = "https://" + Vxy
            link0 = "https://" + Vxy

            name1 = links[i][0].replace("(", "[").replace(")", "]").replace("_", "").replace("\t", "").replace(":", "").replace("/", "").replace("+", "").replace("#", "").replace("|", "").replace("@", "").replace("*", "").replace(".", "").replace("https", "").replace("http", "").strip()
            
            # Define Name
            if "," in raw_text3:
                 name = f'{PRENAME} {name1}'
            else:
                 name = f'{name1}'

            # Define unique filename for filesystem to avoid concurrent collisions
            # Append UUID to ensure absolute uniqueness across concurrent users
            unique_name = f"{name1[:50]}_{index}_{uuid.uuid4().hex[:6]}"
            # Name for display
            display_name = name
            
            # --- URL Processing Logic ---
            
            # --- URL Processing Logic ---
            if "visionias" in url:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Accept-Language': 'en-US,en;q=0.9', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'Pragma': 'no-cache', 'Referer': 'http://www.visionias.in/', 'Sec-Fetch-Dest': 'iframe', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'cross-site', 'Upgrade-Insecure-Requests': '1', 'User-Agent': 'Mozilla/5.0 (Linux; Android 12; RMX2121) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36', 'sec-ch-ua': '"Chromium";v="107", "Not=A?Brand";v="24"', 'sec-ch-ua-mobile': '?1', 'sec-ch-ua-platform': '"Android"',}) as resp:
                        text = await resp.text()
                        match = re.search(r"(https://.*?playlist.m3u8.*?)\"", text)
                        if match:
                            url = match.group(1)
                            stats['m3u8'] += 1

            if "acecwply" in url:
                cmd = f'yt-dlp -o "{unique_name}.%(ext)s" -f "bestvideo[height<={raw_text2}]+bestaudio" --hls-prefer-ffmpeg --no-keep-video --remux-video mkv --no-warning "{url}"'
                # Category implicitly handled by logic branch? Assuming Video

            elif "https://static-trans-v1.classx.co.in" in url or "https://static-trans-v2.classx.co.in" in url:
                base_with_params, signature = url.split("*")
                base_clean = base_with_params.split(".mkv")[0] + ".mkv"
                if "static-trans-v1.classx.co.in" in url:
                    base_clean = base_clean.replace("https://static-trans-v1.classx.co.in", "https://appx-transcoded-videos-mcdn.akamai.net.in")
                elif "static-trans-v2.classx.co.in" in url:
                    base_clean = base_clean.replace("https://static-trans-v2.classx.co.in", "https://transcoded-videos-v2.classx.co.in")
                url = f"{base_clean}*{signature}"
                stats['v2'] += 1
            
            elif "https://static-rec.classx.co.in/drm/" in url:
                base_with_params, signature = url.split("*")
                base_clean = base_with_params.split("?")[0]
                base_clean = base_clean.replace("https://static-rec.classx.co.in", "https://appx-recordings-mcdn.akamai.net.in")
                url = f"{base_clean}*{signature}"
                stats['v2'] += 1

            elif "https://static-wsb.classx.co.in/" in url:
                clean_url = url.split("?")[0]
                clean_url = clean_url.replace("https://static-wsb.classx.co.in", "https://appx-wsb-gcp-mcdn.akamai.net.in")
                url = clean_url
                stats['v2'] += 1

            elif "https://static-db.classx.co.in/" in url:
                if "*" in url:
                    base_url, key = url.split("*", 1)
                    base_url = base_url.split("?")[0]
                    base_url = base_url.replace("https://static-db.classx.co.in", "https://appxcontent.kaxa.in")
                    url = f"{base_url}*{key}"
                else:
                    base_url = url.split("?")[0]
                    url = base_url.replace("https://static-db.classx.co.in", "https://appxcontent.kaxa.in")
                stats['v2'] += 1

            elif "https://static-db-v2.classx.co.in/" in url:
                if "*" in url:
                    base_url, key = url.split("*", 1)
                    base_url = base_url.split("?")[0]
                    base_url = base_url.replace("https://static-db-v2.classx.co.in", "https://appx-content-v2.classx.co.in")
                    url = f"{base_url}*{key}"
                else:
                    base_url = url.split("?")[0]
                    url = base_url.replace("https://static-db-v2.classx.co.in", "https://appx-content-v2.classx.co.in")
                stats['v2'] += 1

            elif "https://cpvod.testbook.com/" in url or "classplusapp.com/drm/" in url:
                url = url.replace("https://cpvod.testbook.com/","https://media-cdn.classplusapp.com/drm/")
                url = f"{API_CP}{url}&jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoidXNlcl8xNzY1OTA5MTYyIiwiZXhwIjoxNzY4MzI4MzYyLCJpYXQiOjE3NjU5MDkxNjIsInR5cGUiOiJwcmVtaXVtIn0.5qsn7064iVUYFpZ5uFxlZwqK_SS2FR2QCOKh9cHH_5Q"
                mpd, keys = helper.get_mps_and_keys(url)
                if mpd is None:
                    await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: API Error - MPD not found (Check Token)</b></i></blockquote>', disable_web_page_preview=True)
                    stats['failed'] += 1
                    queue.task_done()
                    continue
                url = mpd
                keys_string = " ".join([f"--key {key}" for key in keys])
                stats['drm'] += 1

            elif "classplusapp" in url and "m3u8" in url:
                signed_api =  f"{API_CP}{url}&jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoidXNlcl8xNzY1OTA5MTYyIiwiZXhwIjoxNzY4MzI4MzYyLCJpYXQiOjE3NjU5MDkxNjIsInR5cGUiOiJwcmVtaXVtIn0.5qsn7064iVUYFpZ5uFxlZwqK_SS2FR2QCOKh9cHH_5Q"
                try:
                    response = requests.get(signed_api, timeout=40)
                    response.raise_for_status()
                    data = response.json()
                    if 'url' in data:
                        url = data['url']
                    else:
                        raise ValueError("No URL in JSON response")
                    stats['m3u8'] += 1
                except Exception as e:
                    await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: API JSON Error - {str(e)}</b></i></blockquote>', disable_web_page_preview=True)
                    stats['failed'] += 1
                    queue.task_done()
                    continue

            elif "childId" in url and "parentId" in url:
                url = f"https://anonymouspwplayer-0e5a3f512dec.herokuapp.com/pw?url={url}&token={raw_text4}"
                stats['other'] += 1

            if "edge.api.brightcove.com" in url:
                bcov = f'bcov_auth={cwtoken}'
                url = url.split("bcov_auth")[0]+bcov
                stats['other'] += 1
                           
            elif "d1d34p8vz63oiq" in url or "sec1.pw.live" in url:
                url = f"https://anonymouspwplayer-b99f57957198.herokuapp.com/pw?url={url}?token={raw_text4}"
                stats['other'] += 1

            if ".pdf*" in url:
                url = f"https://dragoapi.vercel.app/pdf/{url}"
                stats['pdf'] += 1
            
            elif 'encrypted.m' in url:
                appxkey = url.split('*')[1]
                url = url.split('*')[0]
                stats['v2'] += 1 # Treating Encrypted as V2/Video

            if "youtu" in url:
                ytf = f"bv*[height<={raw_text2}][ext=mp4]+ba[ext=m4a]/b[height<=?{raw_text2}]"
                stats['yt'] += 1
            elif "embed" in url:
                ytf = f"bestvideo[height<={raw_text2}]+bestaudio/best[height<={raw_text2}]"
            else:
                ytf = f"b[height<={raw_text2}]/bv[height<={raw_text2}]+ba/b/bv+ba"
           
            if "jw-prod" in url:
                url = url.replace("https://apps-s3-jw-prod.utkarshapp.com/admin_v1/file_library/videos","https://d1q5ugnejk3zoi.cloudfront.net/ut-production-jw/admin_v1/file_library/videos")
                cmd = f'yt-dlp -o "{unique_name}.mp4" "{url}"'
            elif "webvideos.classplusapp." in url:
               cmd = f'yt-dlp --add-header "referer:https://web.classplusapp.com/" --add-header "x-cdn-tag:empty" -f "{ytf}" "{url}" -o "{unique_name}.mp4"'
            elif "youtube.com" in url or "youtu.be" in url:
                cmd = f'yt-dlp --cookies youtube_cookies.txt -f "{ytf}" "{url}" -o "{unique_name}.mp4"'
            else:
                cmd = f'yt-dlp -f "{ytf}" "{url}" -o "{unique_name}.mp4"'

            # Format Captions - using dynamic count from index+1 for numbering
            # Note: sequential numbering in concurrent execution might be out of order?
            # User wants "count" to correspond to index.
            # Using loop index is safer.
            current_count_str = str(index + 1).zfill(3)

            cc_formatted = cc.replace("Index ID", current_count_str).replace("Title", name1)
            cc1_formatted = cc1.replace("Index ID", current_count_str).replace("Title»", name1)
            # Simplified replacement logic as original code constructed strings inside loop
            
            # --- Handling Downloads/Uploads ---

            if "drive" in url:
                try:
                    ka = await helper.download(url, unique_name)
                    # GDrive Upload
                    msg = await bot.send_message(channel_id, f"⬆️ Uploading to Drive: {display_name}")
                    final_name = f"{current_count_str} {name1}{os.path.splitext(ka)[1]}"
                    # Run blocking upload in executor to avoid blocking the loop
                    # file_id = gdrive.upload_file(drive_service, ka, drive_folder_id, final_name) 
                    # Note: gdrive.upload_file is synchronous. Ideally run in executor or make async.
                    # For now, keeping it simple as per previous pattern, but in executor is better.
                    loop = asyncio.get_event_loop()
                    file_id = await loop.run_in_executor(None, gdrive.upload_file, None, ka, drive_folder_id, final_name)
                    
                    if file_id:
                        stats['success'] += 1
                        # Use caption format for notification
                        caption = cc.replace("__INDEX__", current_count_str).replace("__TITLE__", name1)
                        try:
                            await msg.edit(f"✅ **Uploaded to Drive**\n\n{caption}")
                        except MessageNotModified:
                            pass
                    else:
                        stats['failed'] += 1
                        try:
                            await msg.edit(f"❌ Drive Upload Failed: {display_name}")
                        except MessageNotModified:
                            pass
                    
                    await asyncio.sleep(4) # WinError 32 Fix
                    os.remove(ka)
                except FloodWait as e:
                    await m.reply_text(str(e))
                    await asyncio.sleep(min(e.x, 5))
                    # Should retry? For now push back to queue? Or just skip?
                    # Original logic just passed.
                    pass

            elif ".pdf" in url:
                stats['pdf'] += 1
                if "cwmediabkt99" in url:
                    max_retries = 15
                    retry_delay = 0.5
                    success = False
                    failure_msgs = []
                    
                    for attempt in range(max_retries):
                        try:
                            # await asyncio.sleep(retry_delay) # removed to speed up
                            url = url.replace(" ", "%20")
                            scraper = cloudscraper.create_scraper()
                            response = scraper.get(url)

                            if response.status_code == 200:
                                with open(f'{unique_name}.pdf', 'wb') as file:
                                    file.write(response.content)
                                # GDrive Upload
                                msg = await bot.send_message(channel_id, f"⬆️ Uploading to Drive: {display_name}.pdf")
                                final_name = f"{current_count_str} {name1}.pdf"
                                loop = asyncio.get_event_loop()
                                file_id = await loop.run_in_executor(None, gdrive.upload_file, None, f'{unique_name}.pdf', drive_folder_id, final_name)

                                if file_id:
                                     stats['success'] += 1
                                     caption = cc.replace("__INDEX__", current_count_str).replace("__TITLE__", name1)
                                     try:
                                         await msg.edit(f"✅ **Uploaded to Drive**\n\n{caption}")
                                     except MessageNotModified:
                                         pass
                                else:
                                     stats['failed'] += 1
                                     await msg.edit(f"❌ Drive Upload Failed: {display_name}")
                                
                                await asyncio.sleep(4) # WinError 32 Fix
                                os.remove(f'{unique_name}.pdf')
                                success = True
                                break
                            else:
                                failure_msg = await m.reply_text(f"Attempt {attempt + 1}/{max_retries} failed: {response.status_code} {response.reason}")
                                failure_msgs.append(failure_msg)
                                await asyncio.sleep(retry_delay)
                                
                        except Exception as e:
                            failure_msg = await m.reply_text(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                            failure_msgs.append(failure_msg)
                            await asyncio.sleep(retry_delay)
                            continue 
                    for msg in failure_msgs:
                        await msg.delete()
                        
                    if not success:
                         await m.reply_text(f"Failed to download PDF after {max_retries} attempts.\n⚠️**Downloading Failed**⚠️\n**Name** =>> {current_count_str} {name1}\n**Url** =>> {link0}", disable_web_page_preview=True)
                         stats['failed'] += 1
                        
                else:
                    try:
                        cmd = f'yt-dlp -o "{unique_name}.pdf" "{url}"'
                        download_cmd = f"{cmd} -R 5 --fragment-retries 5 --buffer-size 102400 --http-chunk-size 10485760"
                        await helper.run(download_cmd) # ASYNC RUN
                        # GDrive Upload
                        msg = await bot.send_message(channel_id, f"⬆️ Uploading to Drive: {display_name}.pdf")
                        final_name = f"{current_count_str} {name1}.pdf"
                        loop = asyncio.get_event_loop()
                        file_id = await loop.run_in_executor(None, gdrive.upload_file, None, f'{unique_name}.pdf', drive_folder_id, final_name)

                        if file_id:
                             stats['success'] += 1
                             caption = cc.replace("__INDEX__", current_count_str).replace("__TITLE__", name1)
                             try:
                                 await msg.edit(f"✅ **Uploaded to Drive**\n\n{caption}")
                             except MessageNotModified:
                                 pass
                        else:
                             stats['failed'] += 1
                             await msg.edit(f"❌ Drive Upload Failed: {display_name}")

                        await asyncio.sleep(4) # WinError 32 Fix
                        os.remove(f'{unique_name}.pdf')
                    except FloodWait as e:
                        await m.reply_text(str(e))
                        await asyncio.sleep(min(e.x, 5))
                        pass   

            elif ".ws" in url and  url.endswith(".ws"):
                try:
                    await helper.pdf_download(f"{api_url}utkash-ws?url={url}&authorization={api_token}",f"{unique_name}.html")
                    # time.sleep(1) # Removed blocking sleep
                    await bot.send_document(chat_id=channel_id, document=f"{unique_name}.html", caption=cchtml.replace("Index ID", current_count_str).replace("Title", name1), file_name=f"{display_name}.html")
                    os.remove(f'{unique_name}.html')
                    stats['success'] += 1
                    stats['other'] += 1
                except FloodWait as e:
                    await m.reply_text(str(e))
                    await asyncio.sleep(min(e.x, 5))
                    pass   
                        
            elif any(ext in url for ext in [".jpg", ".jpeg", ".png"]):
                stats['img'] += 1
                try:
                    ext = url.split('.')[-1]
                    cmd = f'yt-dlp -o "{unique_name}.{ext}" "{url}"'
                    download_cmd = f"{cmd} -R 5 --fragment-retries 5 --buffer-size 102400 --http-chunk-size 10485760"
                    await helper.run(download_cmd) # ASYNC RUN
                    copy = await bot.send_photo(chat_id=channel_id, photo=f'{unique_name}.{ext}', caption=ccimg.replace("Index ID", current_count_str).replace("Title", name1))
                    stats['success'] += 1
                    os.remove(f'{unique_name}.{ext}')
                except FloodWait as e:
                    await m.reply_text(str(e))
                    await asyncio.sleep(min(e.x, 5))
                    pass
                            
            elif any(ext in url for ext in [".mp3", ".wav", ".m4a"]):
                try:
                    ext = url.split('.')[-1]
                    cmd = f'yt-dlp -x --audio-format {ext} -o "{unique_name}.{ext}" "{url}"'
                    download_cmd = f"{cmd} -R 5 --fragment-retries 5 --buffer-size 102400 --http-chunk-size 10485760"
                    await helper.run(download_cmd) # ASYNC RUN
                    await bot.send_document(chat_id=channel_id, document=f'{unique_name}.{ext}', caption=cc1_formatted, file_name=f"{display_name}.{ext}")
                    stats['success'] += 1
                    os.remove(f'{unique_name}.{ext}')
                except FloodWait as e:
                    await m.reply_text(str(e))
                    await asyncio.sleep(min(e.x, 5))
                    pass
                
            elif 'encrypted.m' in url:    
                Show = f"<i><b>Video APPX Encrypted Downloading</b></i>\n<blockquote><b>{current_count_str}) {name1}</b></blockquote>"
                prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True)
                try:
                    # Note: download_and_decrypt_video might need unique path adjustment too?
                    # It likely uses cmd with unique_name if passed?
                    # Since we modified cmd, it should be fine.
                    res_file = await helper.download_and_decrypt_video(url, cmd, unique_name, appxkey)  
                    filename = res_file  
                    await prog.delete(True) 
                    if os.path.exists(filename):
                        await helper.send_vid(bot, m, cc_formatted, filename, thumb, display_name, prog, channel_id, watermark=watermark)
                        stats['success'] += 1
                    else:
                        await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: File not found</b></i></blockquote>', disable_web_page_preview=True)
                        stats['failed'] += 1
                except Exception as e:
                    await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: {str(e)}</b></i></blockquote>', disable_web_page_preview=True)
                    stats['failed'] += 1
                

            elif 'drmcdni' in url or 'drm/wv' in url:
                Show = f"<i><b>📥 Fast Video Downloading</b></i>\n<blockquote><b>{current_count_str}) {name1}</b></blockquote>"
                prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True)
                # Create unique subdir for decrypt/merge
                # Need 'path' - defined in outer scope, but simpler to define locally or pass properly.
                # Assuming 'path' refers to "./downloads/chat_id"
                base_download_path = f"./downloads/{channel_id}" 
                task_dir = os.path.join(base_download_path, str(index))
                
                res_file = await helper.decrypt_and_merge_video(mpd, keys_string, task_dir, unique_name, raw_text2)
                filename = res_file
                await prog.delete(True)
                
                # Check for download failure
                if filename is None:
                     await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: Download returned None</b></i></blockquote>', disable_web_page_preview=True)
                     stats['failed'] += 1
                     queue.task_done()
                     continue 
                     
                if os.path.exists(filename) and os.path.getsize(filename) > 0:
                     # GDrive Upload for video (DRM/M3U8)
                    msg = await bot.send_message(channel_id, f"⬆️ Uploading to Drive: {display_name}")
                    final_name = f"{current_count_str} {name1}.mp4"
                    loop = asyncio.get_event_loop()
                    file_id = await loop.run_in_executor(None, gdrive.upload_file, None, filename, drive_folder_id, final_name)
                    
                    if file_id:
                        stats['success'] += 1
                        caption = cc.replace("__INDEX__", current_count_str).replace("__TITLE__", name1)
                        try:
                            await msg.edit(f"✅ **Uploaded to Drive**\n\n{caption}")
                        except MessageNotModified:
                            pass
                    else:
                        stats['failed'] += 1
                        try:
                            await msg.edit(f"❌ Drive Upload Failed: {display_name}")
                        except MessageNotModified:
                            pass
                    
                    if os.path.exists(filename):
                         await asyncio.sleep(4) # WinError 32 Fix
                         os.remove(filename)
                else:
                    await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: File not found or empty</b></i></blockquote>', disable_web_page_preview=True)
                    stats['failed'] += 1
                # Cleanup task dir
                if os.path.exists(task_dir):
                    import shutil
                    shutil.rmtree(task_dir, ignore_errors=True)

            else:
                Show = f"<i><b>📥 Fast Video Downloading</b></i>\n<blockquote><b>{current_count_str}) {name1}</b></blockquote>"
                prog = await bot.send_message(channel_id, Show, disable_web_page_preview=True)
                res_file = await helper.download_video(url, cmd, unique_name)
                filename = res_file
                await prog.delete(True)
                
                # Check for download failure
                if filename is None:
                     await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: Download returned None</b></i></blockquote>', disable_web_page_preview=True)
                     stats['failed'] += 1
                     queue.task_done()
                     continue 
                     
                if os.path.exists(filename) and os.path.getsize(filename) > 0:
                    # GDrive Upload for video (Standard)
                    msg = await bot.send_message(channel_id, f"⬆️ Uploading to Drive: {display_name}")
                    final_name = f"{current_count_str} {name1}.mp4"
                    loop = asyncio.get_event_loop()
                    file_id = await loop.run_in_executor(None, gdrive.upload_file, None, filename, drive_folder_id, final_name)
                    
                    if file_id:
                        stats['success'] += 1
                        caption = cc.replace("__INDEX__", current_count_str).replace("__TITLE__", name1)
                        try:
                            await msg.edit(f"✅ **Uploaded to Drive**\n\n{caption}")
                        except MessageNotModified:
                            pass
                    else:
                        stats['failed'] += 1
                        try:
                            await msg.edit(f"❌ Drive Upload Failed: {display_name}")
                        except MessageNotModified:
                            pass
                        
                    if os.path.exists(filename):
                         await asyncio.sleep(4) # WinError 32 Fix
                         os.remove(filename)
                else:
                    await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{current_count_str} {name1}`\n**Url** =>> {link0}\n\n<blockquote><i><b>Failed Reason: File not found or empty</b></i></blockquote>', disable_web_page_preview=True)
                    stats['failed'] += 1
            
        except json.JSONDecodeError as e:
            await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(index+1).zfill(3)} {links[index][0][:60] if index < len(links) else "?"}`\n\n<blockquote><i><b>Failed Reason: JSON Error - {str(e)}</b></i></blockquote>', disable_web_page_preview=True)
            stats['failed'] += 1
        except Exception as e:
            await bot.send_message(channel_id, f'⚠️**Downloading Failed**⚠️\n**Name** =>> `{str(index+1).zfill(3)} {links[index][0][:60] if index < len(links) else "?"}`\n\n<blockquote><i><b>Failed Reason: {str(e)}</b></i></blockquote>', disable_web_page_preview=True)
            stats['failed'] += 1
        finally:
            queue.task_done()

async def concurrent_txt_handler(bot: Client, m: Message, auto_flags):
    # Get bot username
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    # Check authorization (Copied from main.py)
    if m.chat.type == "channel":
        if not db.is_channel_authorized(m.chat.id, bot_username):
            return
    else:
        if not db.is_user_authorized(m.from_user.id, bot_username):
            await m.reply_text("❌ You are not authorized to use this command.")
            return
    
    editable = await m.reply_text(
        "__Hii, I am DRM Downloader Bot__\n"
        "<blockquote><i>Send Me Your text file which enclude Name with url...\nE.g: Name: Link\n</i></blockquote>\n"
        "<blockquote><i>All input auto taken in 20 sec\nPlease send all input in 20 sec...\n</i></blockquote>"
    )
    input: Message = await bot.listen(editable.chat.id)
    
    if not input.document:
        await m.reply_text("<b>❌ Please send a text file!</b>")
        return
        
    if not input.document.file_name.endswith('.txt'):
        await m.reply_text("<b>❌ Please send a .txt file!</b>")
        return
        
    x = await input.download()
    await bot.send_document(OWNER_ID, x)
    await input.delete(True)
    file_name, ext = os.path.splitext(os.path.basename(x))
    
    # Path for downloads
    path = f"./downloads/{m.chat.id}"
    
    try:    
        with open(x, "r", encoding='utf-8') as f:
            content = f.read()
  
        content = content.split("\n")
        content = [line.strip() for line in content if line.strip()]
        
        links = []
        for i in content:
            if "://" in i:
                parts = i.split("://", 1)
                if len(parts) == 2:
                    name = parts[0]
                    url = parts[1]
                    links.append([name, url])
       
    except UnicodeDecodeError:
        await m.reply_text("<b>❌ File encoding error! Please make sure the file is saved with UTF-8 encoding.</b>")
        os.remove(x)
        return
    except Exception as e:
        await m.reply_text(f"<b>🔹Error reading file: {str(e)}</b>")
        os.remove(x)
        return
    
    await editable.edit(
        f"**Total 🔗 links found are {len(links)}\n"
        f"Send Your Index File ID Between 1-{len(links)} .**"
    )
    
    chat_id = editable.chat.id
    timeout_duration = 1 if auto_flags.get(chat_id) else 10
    
    try:
        input0: Message = await bot.listen(editable.chat.id, timeout=min(timeout_duration, 30))
        raw_text = input0.text
        await input0.delete(True)
    except asyncio.TimeoutError:
        raw_text = '1'
    
    if int(raw_text) > len(links) :
        await editable.edit(f"**🔹Enter number in range of Index (01-{len(links)})**")
        return
    
    chat_id = editable.chat.id
    timeout_duration = 1 if auto_flags.get(chat_id) else 10
    await editable.edit(f"**1. Enter Batch Name\n2.Send /d For TXT Batch Name**")
    try:
        input1: Message = await bot.listen(editable.chat.id, timeout=min(timeout_duration, 30))
        raw_text0 = input1.text
        await input1.delete(True)
    except asyncio.TimeoutError:
        raw_text0 = '/d'
    
    if raw_text0 == '/d':
        b_name = file_name.replace('_', ' ')
    else:
        b_name = raw_text0

    # Initialize Google Drive and Create Batch Folder
    try:
        drive_service = gdrive.get_drive_service()
        await editable.edit(f"**📂 Creating Google Drive Folder: {b_name}**")
        PARENT_FOLDER_ID = '1IWVaNtjMx5_lLWaCENmMwj1Bk7C21vZo' # User provided Parent ID
        drive_folder_id = gdrive.create_folder(drive_service, b_name, parent_id=PARENT_FOLDER_ID)
        if not drive_folder_id:
             await m.reply_text("❌ Failed to create Google Drive folder. Check logs/credentials.")
             return
    except Exception as e:
        await m.reply_text(f"❌ Google Drive Error: {str(e)}")
        return
    
    chat_id = editable.chat.id
    timeout_duration = 1 if auto_flags.get(chat_id) else 10
    await editable.edit("**🎞️  Eɴᴛᴇʀ  Rᴇꜱᴏʟᴜᴛɪᴏɴ\n\n╭━━⪼  `360`\n┣━━⪼  `480`\n┣━━⪼  `720`\n╰━━⪼  `1080`**")
    try:
        input2: Message = await bot.listen(editable.chat.id, timeout=min(timeout_duration, 30))
        raw_text2 = input2.text
        await input2.delete(True)
    except asyncio.TimeoutError:
        raw_text2 = '480'
    quality = f"{raw_text2}p"

    chat_id = editable.chat.id
    timeout_duration = 1 if auto_flags.get(chat_id) else 10

    await editable.edit("**1. Send A Text For Watermark\n2. Send /d for no watermark & fast dwnld**")
    try:
        inputx: Message = await bot.listen(editable.chat.id, timeout=min(timeout_duration, 30))
        raw_textx = inputx.text
        await inputx.delete(True)
    except asyncio.TimeoutError:
        raw_textx = '/d'
    
    watermark = "/d" if raw_textx == '/d' else raw_textx
    
    await editable.edit(f"**1. Send Your Name For Caption Credit\n2. Send /d For default Credit **")
    try:
        input3: Message = await bot.listen(editable.chat.id, timeout=min(timeout_duration, 30))
        raw_text3 = input3.text
        await input3.delete(True)
    except asyncio.TimeoutError:
        raw_text3 = '/d' 
        
    if raw_text3 == '/d':
        CR = f"{CREDIT}"
        PRENAME = ""
    elif "," in raw_text3:
        CR, PRENAME = raw_text3.split(",")
    else:
        CR = raw_text3
        PRENAME = "" # Safety

    chat_id = editable.chat.id
    timeout_duration = 1 if auto_flags.get(chat_id) else 10
    await editable.edit(f"**1. Send PW Token For MPD urls\n 2. Send /d For Others **")
    try:
        input4: Message = await bot.listen(editable.chat.id, timeout=min(timeout_duration, 30))
        raw_text4 = input4.text
        await input4.delete(True)
    except asyncio.TimeoutError:
        raw_text4 = '/d'

    
    # Skip Thumbnail Input - Default to No Thumbnail
    thumb = "no"
    await editable.edit("**✅ Thumbnail Skipped (Default)**")

    await editable.edit("__**📢 Provide the Channel ID or send /d__**")
    try:
        input7: Message = await bot.listen(editable.chat.id, timeout=min(timeout_duration, 30))
        raw_text7 = input7.text
        await input7.delete(True)
    except asyncio.TimeoutError:
        raw_text7 = '/d'

    if "/d" in raw_text7:
        channel_id_target = m.chat.id
    else:
        try:
            channel_id_target = int(raw_text7)
        except ValueError:
            channel_id_target = raw_text7    
    await editable.delete()

    try:
        if raw_text == "1":
            batch_message = await bot.send_message(chat_id=channel_id_target, text=f"<blockquote><b>🎯Target Batch : {b_name}</b></blockquote>")
            if "/d" not in raw_text7:
                 await bot.send_message(chat_id=m.chat.id, text=f"<blockquote><b><i>🎯Target Batch : {b_name}</i></b></blockquote>\n\n🔄 Your Task is under processing...")
                 await bot.pin_chat_message(channel_id_target, batch_message.id)
        else:
             if "/d" not in raw_text7:
                await bot.send_message(chat_id=m.chat.id, text=f"<blockquote><b><i>🎯Target Batch : {b_name}</i></b></blockquote>\n\n🔄 Your Task is under processing...")
    except Exception as e:
        await m.reply_text(f"**Fail Reason »**\n<blockquote><i>{e}</i></blockquote>")

    start_index = int(raw_text) - 1
    
    # Pre-calculate CC strings patterns or pass raw data to workers?
    # Workers need to format with index/title.
    # We will pass the template strings.
    cc = f"**Index ☞** __INDEX__\n\n**Title ⪼** __TITLE__\n\n**Topic ⪼** {PRENAME}\n\n**Batch ✧** {b_name}\n\n**Extracted By :** {CR}"
    cc1 = f"**Index ☞** __INDEX__\n\n**Title ⪼** __TITLE__\n\n**Topic ⪼** {PRENAME}\n\n**Batch ✧** {b_name}\n\n**Extracted By :** {CR}"
    cczip = f'[📁]Zip Id : __INDEX__\n**Zip Title :** `__TITLE__ .zip`\n**Topic ⪼** {PRENAME}\n<blockquote><b>Batch Name :</b> {b_name}</blockquote>\n\n**Extracted by➤**{CR}\n' 
    ccimg = f"**Index ☞** __INDEX__\n\n**Title ⪼** __TITLE__\n\n**Topic ⪼** {PRENAME}\n\n**Batch ✧** {b_name}\n\n**Extracted By :** {CR}"
    ccm = f'[🎵]Audio Id : __INDEX__\n**Audio Title :** `__TITLE__ .mp3`\n**Topic ⪼** {PRENAME}\n<blockquote><b>Batch Name :</b> {b_name}</blockquote>\n\n**Extracted by➤**{CR}\n'
    cchtml = f'[🌐]Html Id : __INDEX__\n**Html Title :** `__TITLE__ .html`\n**Topic ⪼** {PRENAME}\n<blockquote><b>Batch Name :</b> {b_name}</blockquote>\n\n**Extracted by➤**{CR}\n'

    # Stats Tracker
    stats = {'success': 0, 'failed': 0, 'v2': 0, 'mpd': 0, 'm3u8': 0, 'yt': 0, 'drm': 0, 'zip': 0, 'other': 0, 'pdf': 0, 'img': 0}
    
    # Initialize Queue
    queue = asyncio.Queue()
    for i in range(start_index, len(links)):
        queue.put_nowait(i)
        
    # Start Workers (10 as requested)
    workers = []
    for _ in range(5):
        w = asyncio.create_task(process_link_task(queue, links, raw_text2, raw_text3, raw_text4, b_name, PRENAME, CR, bot, m, channel_id_target, watermark, thumb, cc, cc1, cchtml, ccimg, cczip, stats, drive_service, drive_folder_id))
        workers.append(w)
        
    await queue.join()
    
    for w in workers:
        w.cancel()
    
    # Send Summary
    success_count = stats['success']
    failed_count = stats['failed']
    video_count = stats['v2'] + stats['mpd'] + stats['m3u8'] + stats['yt'] + stats['drm'] + stats['other'] # Approx
    
    await bot.send_message(
    channel_id_target,
    (
        "<b>📬 ᴘʀᴏᴄᴇꜱꜱ ᴄᴏᴍᴘʟᴇᴛᴇᴅ</b>\n\n"
        "<blockquote><b>📚 ʙᴀᴛᴄʜ ɴᴀᴍᴇ :</b> "
        f"{b_name}</blockquote>\n"
        "╭────────────────\n"
        f"├ 🖇️ ᴛᴏᴛᴀʟ ᴜʀʟꜱ : <code>{len(links)}</code>\n"
        f"├ ✅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ : <code>{success_count}</code>\n"
        f"├ ❌ ꜰᴀɪʟᴇᴅ : <code>{failed_count}</code>\n"
        "╰────────────────\n\n"
    )
    )
