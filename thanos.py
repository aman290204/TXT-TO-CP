import os
import re
import time
import mmap
import datetime
import aiohttp
import aiofiles
import asyncio
import logging
import requests
# import tgcrypto  # Removed to avoid dependency issues
import subprocess
import concurrent.futures
from math import ceil
from utils import progress_bar
from pyrogram import Client, filters
from pyrogram.types import Message
from io import BytesIO
from pathlib import Path  
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
import math
import m3u8
from urllib.parse import urljoin
from vars import *  # Add this import
from db import Database
import asyncio

# Global Semaphore for controlling concurrent downloads
download_sem = asyncio.Semaphore(5)  # Increased from 3 to 5 for better concurrency


def get_duration(filename):
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30  # Add timeout
        )
        if result.returncode == 0 and result.stdout:
            return float(result.stdout)
        else:
            print(f"⚠️ Could not get duration for {filename}, return code: {result.returncode}")
            return 0.0
    except subprocess.TimeoutExpired:
        print(f"⚠️ ffprobe timeout for {filename}")
        return 0.0
    except Exception as e:
        print(f"⚠️ Error getting duration for {filename}: {str(e)}")
        return 0.0


def split_large_video(file_path, max_size_mb=1900):
    try:
        size_bytes = os.path.getsize(file_path)
        max_bytes = max_size_mb * 1024 * 1024

        if size_bytes <= max_bytes:
            return [file_path]  # No splitting needed

        duration = get_duration(file_path)
        parts = ceil(size_bytes / max_bytes)
        part_duration = duration / parts
        base_name = file_path.rsplit(".", 1)[0]
        output_files = []

        for i in range(parts):
            output_file = f"{base_name}_part{i+1}.mp4"
            cmd = [
                "ffmpeg",
                "-i", file_path,
                "-ss", str(int(part_duration * i)),
                "-t", str(int(part_duration)),
                "-c", "copy",
                output_file
            ]
            try:
                result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=300)  # 5 minute timeout
                if result.returncode == 0 and os.path.exists(output_file):
                    output_files.append(output_file)
                else:
                    print(f"⚠️ Failed to create part {i+1}, return code: {result.returncode}")
            except subprocess.TimeoutExpired:
                print(f"⚠️ ffmpeg timeout creating part {i+1}")
            except Exception as e:
                print(f"⚠️ Error creating part {i+1}: {str(e)}")

        if not output_files:
            print(f"❌ No parts were successfully created, returning original file")
            return [file_path]
            
        return output_files
    except Exception as e:
        print(f"⚠️ Error in split_large_video: {str(e)}")
        # Return original file if splitting fails
        return [file_path]


def duration(filename):
    try:
        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                 "format=duration", "-of",
                                 "default=noprint_wrappers=1:nokey=1", filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30)
        if result.returncode == 0 and result.stdout:
            return float(result.stdout)
        else:
            print(f"⚠️ Could not get duration for {filename}, return code: {result.returncode}")
            return 0.0
    except subprocess.TimeoutExpired:
        print(f"⚠️ ffprobe timeout for {filename}")
        return 0.0
    except Exception as e:
        print(f"⚠️ Error getting duration for {filename}: {str(e)}")
        return 0.0


def get_mps_and_keys(api_url):
    response = requests.get(api_url)
    response_json = response.json()
    mpd = response_json.get('mpd_url')
    keys = response_json.get('keys')
    return mpd, keys


   
def exec(cmd):
        process = subprocess.run(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        output = process.stdout.decode()
        print(output)
        return output
        #err = process.stdout.decode()
def pull_run(work, cmds):
    with concurrent.futures.ThreadPoolExecutor(max_workers=work) as executor:
        print("Waiting for tasks to complete")
        fut = executor.map(exec,cmds)
async def aio(url,name):
    k = f'{name}.pdf'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(k, mode='wb')
                await f.write(await resp.read())
                await f.close()
    return k


async def download(url,name):
    ka = f'{name}.pdf'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(ka, mode='wb')
                await f.write(await resp.read())
                await f.close()
    return ka

async def pdf_download(url, file_name, chunk_size=1024 * 10):
    if os.path.exists(file_name):
        os.remove(file_name)
    r = requests.get(url, allow_redirects=True, stream=True)
    with open(file_name, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                fd.write(chunk)
    return file_name   
   

def parse_vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = []
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ",2)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.append((i[0], i[2]))
            except:
                pass
    return new_info


def vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = dict()
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ",3)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    
                    # temp.update(f'{i[2]}')
                    # new_info.append((i[2], i[0]))
                    #  mp4,mkv etc ==== f"({i[1]})" 
                    
                    new_info.update({f'{i[2]}':f'{i[0]}'})

            except:
                pass
    return new_info


async def decrypt_and_merge_video(mpd_url, keys_string, output_path, output_name, quality="720"):
    try:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        cmd1 = f'yt-dlp -f "bv[height<={quality}]+ba/b" -o "{output_path}/file.%(ext)s" --allow-unplayable-format --no-check-certificate --external-downloader aria2c --write-thumbnail --convert-thumbnails jpg "{mpd_url}"'
        print(f"Running command: {cmd1}")
        print(f"Running command: {cmd1}")
        async with download_sem:
            print(f"⬇️ Starting download... (Semaphore Acquired)")
            await run(cmd1)
        
        avDir = list(output_path.iterdir())
        print(f"Downloaded files: {avDir}")
        print("Decrypting")

        video_decrypted = False
        audio_decrypted = False

        for data in avDir:
            if data.suffix == ".mp4" and not video_decrypted:
                cmd2 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/video.mp4"'
                print(f"Running command: {cmd2}")
                print(f"Running command: {cmd2}")
                await run(cmd2)
                if (output_path / "video.mp4").exists():
                    video_decrypted = True
                data.unlink()
            elif data.suffix == ".m4a" and not audio_decrypted:
                cmd3 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                print(f"Running command: {cmd3}")
                print(f"Running command: {cmd3}")
                await run(cmd3)
                if (output_path / "audio.m4a").exists():
                    audio_decrypted = True
                data.unlink()

        if not video_decrypted or not audio_decrypted:
            raise FileNotFoundError("Decryption failed: video or audio file not found.")

        cmd4 = f'ffmpeg -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy "{output_path}/{output_name}.mp4"'
        print(f"Running command: {cmd4}")
        print(f"Running command: {cmd4}")
        await run(cmd4)
        if (output_path / "video.mp4").exists():
            (output_path / "video.mp4").unlink()
        if (output_path / "audio.m4a").exists():
            (output_path / "audio.m4a").unlink()
        
        filename = output_path / f"{output_name}.mp4"

        if not filename.exists():
            raise FileNotFoundError("Merged video file not found.")

        cmd5 = f'ffmpeg -i "{filename}" 2>&1 | grep "Duration"'
        duration_info = os.popen(cmd5).read()
        print(f"Duration info: {duration_info}")

        return str(filename)

    except Exception as e:
        print(f"Error during decryption and merging: {str(e)}")
        raise

async def run(cmd):
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        
        # Add timeout for the subprocess
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180.0)  # 3 minute timeout

        print(f'[{cmd!r} exited with {proc.returncode}]')
        if proc.returncode == 1:
            return False
        if stdout:
            return f'[stdout]\n{stdout.decode()}'
        if stderr:
            return f'[stderr]\n{stderr.decode()}'
            
    except asyncio.TimeoutError:
        print(f"[{cmd!r} timed out after 3 minutes]")
        return False
    except Exception as e:
        print(f"[{cmd!r} failed with exception: {str(e)}]")
        return False

def old_download(url, file_name, chunk_size = 1024 * 10 * 10):
    try:
        # Remove existing file if it exists
        if os.path.exists(file_name):
            os.remove(file_name)
            
        # Add timeout and better error handling
        r = requests.get(url, allow_redirects=True, stream=True, timeout=60)
        r.raise_for_status()  # Raise an exception for bad status codes
        
        with open(file_name, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    fd.write(chunk)
        
        # Check if file was downloaded successfully
        if os.path.exists(file_name) and os.path.getsize(file_name) > 0:
            return file_name
        else:
            print(f"⚠️ Downloaded file is empty or missing: {file_name}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request error during download: {str(e)}")
        return None
    except Exception as e:
        print(f"⚠️ Error during download: {str(e)}")
        return None


def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


def time_name():
    date = datetime.date.today()
    now = datetime.datetime.now()
    current_time = now.strftime("%H%M%S")
    return f"{date} {current_time}.mp4"


async def fast_download(url, name):
    """Fast direct download implementation without yt-dlp"""
    max_retries = 5
    retry_count = 0
    success = False
    
    while not success and retry_count < max_retries:
        try:
            if "m3u8" in url:
                # Handle m3u8 files
                async with download_sem:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url) as response:
                            m3u8_text = await response.text()
                        
                    playlist = m3u8.loads(m3u8_text)
                    if playlist.is_endlist:
                        # Direct download of segments
                        base_url = url.rsplit('/', 1)[0] + '/'
                        
                        # Download all segments concurrently
                        segments = []
                        async with aiohttp.ClientSession() as session:
                            tasks = []
                            for segment in playlist.segments:
                                segment_url = urljoin(base_url, segment.uri)
                                task = asyncio.create_task(session.get(segment_url))
                                tasks.append(task)
                            
                            responses = await asyncio.gather(*tasks)
                            for response in responses:
                                segment_data = await response.read()
                                segments.append(segment_data)
                        
                        # Merge segments and save
                        output_file = f"{name}.mp4"
                        with open(output_file, 'wb') as f:
                            for segment in segments:
                                f.write(segment)
                        
                        success = True
                        return [output_file]
                    else:
                        # For live streams, fall back to ffmpeg
                        cmd = f'ffmpeg -hide_banner -loglevel error -stats -i "{url}" -c copy -bsf:a aac_adtstoasc -movflags +faststart "{name}.mp4"'
                        subprocess.run(cmd, shell=True)
                        if os.path.exists(f"{name}.mp4"):
                            success = True
                            return [f"{name}.mp4"]
            else:
                # For direct video URLs
                async with download_sem:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url) as response:
                            if response.status == 200:
                                output_file = f"{name}.mp4"
                                with open(output_file, 'wb') as f:
                                    while True:
                                        chunk = await response.content.read(1024*1024)  # 1MB chunks
                                        if not chunk:
                                            break
                                        f.write(chunk)
                                success = True
                                return [output_file]
            
            if not success:
                print(f"\nAttempt {retry_count + 1} failed, retrying in 3 seconds...")
                retry_count += 1
                await asyncio.sleep(0.5)
                
        except Exception as e:
            print(f"\nError during attempt {retry_count + 1}: {str(e)}")
            retry_count += 1
            await asyncio.sleep(0.5)
    
    return None

async def download_video(url, cmd, name):
    retry_count = 0
    max_retries = 3  # Increase retries
    
    while retry_count < max_retries:
        # Add timeout and better error handling
        download_cmd = f'{cmd} -R 5 --fragment-retries 5 --buffer-size 102400 --http-chunk-size 10485760 --external-downloader aria2c --downloader-args "aria2c: -x 8 -j 8 -s 8" --write-thumbnail --convert-thumbnails jpg --socket-timeout 60 --retries 3'
        print(download_cmd)
        logging.info(download_cmd)
        
        async with download_sem:
            print(f"⬇️ Starting download... (Semaphore Acquired)")
            try:
                # Add timeout to the subprocess
                k = await asyncio.wait_for(run(download_cmd), timeout=120.0)  # 2 minute timeout
            except asyncio.TimeoutError:
                print(f"⚠️ Download timed out (attempt {retry_count + 1}/{max_retries})")
                retry_count += 1
                await asyncio.sleep(2)
                continue
            except Exception as e:
                print(f"⚠️ Download error: {str(e)} (attempt {retry_count + 1}/{max_retries})")
                retry_count += 1
                await asyncio.sleep(2)
                continue

        if k != False:
            break  # success

        retry_count += 1
        print(f"⚠️ Download failed (attempt {retry_count}/{max_retries}), retrying in 2s...")
        await asyncio.sleep(2)
    
    # Check if file exists and is not empty
    try:
        if os.path.isfile(name) and os.path.getsize(name) > 0:
            return name
        elif os.path.isfile(f"{name}.webm") and os.path.getsize(f"{name}.webm") > 0:
            return f"{name}.webm"
        name_no_ext = name.split(".")[0]
        if os.path.isfile(f"{name_no_ext}.mkv") and os.path.getsize(f"{name_no_ext}.mkv") > 0:
            return f"{name_no_ext}.mkv"
        elif os.path.isfile(f"{name_no_ext}.mp4") and os.path.getsize(f"{name_no_ext}.mp4") > 0:
            return f"{name_no_ext}.mp4"
        elif os.path.isfile(f"{name_no_ext}.mp4.webm") and os.path.getsize(f"{name_no_ext}.mp4.webm") > 0:
            return f"{name_no_ext}.mp4.webm"
        
        # If we get here, the file wasn't downloaded properly
        print(f"❌ File not found or empty: {name}")
        return None
    except Exception as exc:
        logging.error(f"Error checking file: {exc}")
        return None


async def send_vid(bot: Client, m: Message, cc, filename, thumb, name, prog, channel_id, watermark="Thanos", topic_thread_id: int = None):
    try:
        temp_thumb = None  # ✅ Ensure this is always defined for later cleanup

        thumbnail = thumb
        if thumb == "no":
            thumbnail = None
        elif thumb == "/d" or not os.path.exists(thumb):
            # First check if yt-dlp already generated a thumbnail during download
            base_filename = os.path.splitext(filename)[0]
            possible_thumbnails = [
                f"{base_filename}.jpg",
                f"{base_filename}.png",
                f"{base_filename}.webp"
            ]
            
            yt_dlp_thumbnail = None
            for thumb_file in possible_thumbnails:
                if os.path.exists(thumb_file):
                    yt_dlp_thumbnail = thumb_file
                    break
            
            if yt_dlp_thumbnail:
                # Use the thumbnail generated by yt-dlp
                thumbnail = yt_dlp_thumbnail
            else:
                # Need to generate thumbnail manually
                # Sanitize filename to avoid special characters that cause issues with FFmpeg
                sanitized_filename = re.sub(r'[<>:"/\\|?*\[\]]', '_', os.path.basename(filename))
                temp_thumb = f"downloads/thumb_{sanitized_filename}.jpg"
                
                # Check if we've already generated this thumbnail
                if os.path.exists(temp_thumb):
                    print(f"Using existing thumbnail: {temp_thumb}")
                    thumbnail = temp_thumb
                else:
                    # Generate thumbnail at 10s - optimized for speed
                    # Properly escape filenames for FFmpeg
                    escaped_filename = filename.replace('"', '\\"')
                    escaped_temp_thumb = temp_thumb.replace('"', '\\"')
                    
                    # Check if the video file exists and is not empty
                    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
                        print(f"Video file {filename} does not exist or is empty")
                        thumbnail = None
                    else:
                        # But for very short videos, use a different approach
                        import subprocess
                        import json
                        
                        # Get video duration
                        seek_time = 10  # default value
                        try:
                            probe_cmd = f'ffprobe -v quiet -print_format json -show_format "{escaped_filename}"'
                            result = subprocess.run(probe_cmd, shell=True, capture_output=True, text=True, timeout=10)
                            if result.returncode == 0:
                                probe_data = json.loads(result.stdout)
                                video_duration = float(probe_data['format']['duration'])
                                
                                # For videos shorter than 5 seconds, grab frame at 1/3 of duration
                                if video_duration < 5:
                                    seek_time = max(0.1, video_duration / 3)
                                else:
                                    seek_time = min(10, video_duration / 10)  # 10% of video or 10s, whichever is smaller
                        except Exception as e:
                            print(f"Could not determine video duration: {e}")
                            seek_time = 10  # fallback to 10 seconds
                        
                        # Format seek time as HH:MM:SS
                        hours = int(seek_time // 3600)
                        minutes = int((seek_time % 3600) // 60)
                        seconds = int(seek_time % 60)
                        seek_time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                        
                        # Use fastest FFmpeg command for thumbnail extraction
                        try:
                            # Use await run instead of blocking subprocess.run
                            cmd_thumb = f'ffmpeg -ss {seek_time_str} -i "{escaped_filename}" -frames:v 1 -c:v mjpeg -y "{escaped_temp_thumb}"'
                            res = await run(cmd_thumb)
                            if res is False:
                                # Fallback to keyframe-based extraction
                                print("Thumbnail generation failed, trying keyframe-based extraction")
                                await run(
                                    f'ffmpeg -skip_frame nokey -i "{escaped_filename}" -frames:v 1 -c:v mjpeg -y "{escaped_temp_thumb}"'
                                )
                        except Exception as e:
                            print(f"Thumbnail generation failed: {e}")
                    
                    # ✅ Only apply watermark if watermark != "/d"
                    # To disable watermarking entirely for faster processing, pass watermark="/d"
                    if os.path.exists(temp_thumb) and (watermark and watermark.strip() != "/d"):
                        text_to_draw = watermark.strip()
                        try:
                            # Probe image width for better scaling
                            # Properly escape thumbnail filename for ffprobe
                            escaped_temp_thumb = temp_thumb.replace('"', '\\"')
                            probe_out = subprocess.check_output(
                                f'ffprobe -v error -select_streams v:0 -show_entries stream=width -of csv=p=0:s=x "{escaped_temp_thumb}"',
                                shell=True,
                                stderr=subprocess.DEVNULL,
                            ).decode().strip()
                            img_width = int(probe_out.split('x')[0]) if 'x' in probe_out else int(probe_out)
                        except Exception:
                            img_width = 1280

                        # Base size relative to width, then adjust by text length
                        base_size = max(28, int(img_width * 0.075))
                        text_len = len(text_to_draw)
                        if text_len <= 3:
                            font_size = int(base_size * 1.25)
                        elif text_len <= 8:
                            font_size = int(base_size * 1.0)
                        elif text_len <= 15:
                            font_size = int(base_size * 0.85)
                        else:
                            font_size = int(base_size * 0.7)
                        font_size = max(32, min(font_size, 120))

                        box_h = max(60, int(font_size * 1.6))

                        # Simple escaping for single quotes in text
                        safe_text = text_to_draw.replace("'", "\\'")

                        # Properly escape thumbnail filename for FFmpeg
                        escaped_temp_thumb = temp_thumb.replace('"', '\\"')
                        
                        text_cmd = (
                            f'ffmpeg -i "{escaped_temp_thumb}" -vf '
                            f'"drawbox=y=0:color=black@0.35:width=iw:height={box_h}:t=fill,'
                            f'drawtext=fontfile=font.ttf:text=\'{safe_text}\':fontcolor=white:'
                            f'fontsize={font_size}:x=(w-text_w)/2:y=(({box_h})-text_h)/2" '
                            f'-c:v mjpeg -q:v 2 -y "{escaped_temp_thumb}"'
                        )
                        # Add timeout to prevent hanging and simplify error handling
                        # Add timeout to prevent hanging and simplify error handling
                        try:
                            await asyncio.wait_for(run(text_cmd), timeout=30)
                        except asyncio.TimeoutError:
                            print("Thumbnail watermarking timed out, using unwatermarked thumbnail")
                        except Exception as e:
                            print(f"Thumbnail watermarking failed: {e}")
                    thumbnail = temp_thumb if os.path.exists(temp_thumb) else None

        await prog.delete(True)  # ⏳ Remove previous progress message

        reply1 = await bot.send_message(channel_id, f" **Uploading Video:**\n<blockquote>{name}</blockquote>")
        reply = await m.reply_text(f"🖼 **Generating Thumbnail:**\n<blockquote>{name}</blockquote>")

        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        notify_split = None
        sent_message = None

        # Wait a moment to ensure file is fully written
        await asyncio.sleep(0.5)
        
        # Check if file is locked by another process
        file_locked = True
        retry_count = 0
        max_retries = 5
        
        while file_locked and retry_count < max_retries:
            try:
                # Try to open the file in read mode to check if it's locked
                with open(filename, 'rb') as test_file:
                    test_file.read(1)  # Try to read a byte
                file_locked = False  # File is not locked
            except IOError as e:
                if "being used by another process" in str(e) or e.errno == 32:  # WinError 32
                    print(f"⚠️ File is locked by another process, waiting... (attempt {retry_count + 1}/{max_retries})")
                    retry_count += 1
                    await asyncio.sleep(2)  # Wait 2 seconds before retrying
                else:
                    # Some other error occurred
                    print(f"⚠️ Error accessing file: {str(e)}")
                    break
        
        if file_locked:
            print(f"❌ File is still locked after {max_retries} attempts: {filename}")
            await reply.edit_text(f"⚠️ Upload failed: File is locked by another process")
            return None

        if file_size_mb < 2000:
            # 📹 Upload as single video
            try:
                dur = int(get_duration(filename))
            except Exception as e:
                print(f"⚠️ Could not get duration, using default: {str(e)}")
                dur = 0
            start_time = time.time()

            # Determine file extension from actual filename
            _, ext = os.path.splitext(filename)
            display_filename = f"{name}{ext}"

            try:
                sent_message = await bot.send_video(
                    chat_id=channel_id,
                    video=filename,
                    caption=cc,
                    file_name=display_filename,
                    supports_streaming=True,
                    height=720,
                    width=1280,
                    thumb=thumbnail,
                    duration=dur,
                    progress=progress_bar,
                    progress_args=(reply, start_time)
                )
            except Exception as e:
                print(f"⚠️ Video upload failed: {str(e)}")
                try:
                    sent_message = await bot.send_document(
                        chat_id=channel_id,
                        document=filename,
                        caption=cc,
                        file_name=display_filename,
                        progress=progress_bar,
                        progress_args=(reply, start_time)
                    )
                except Exception as e2:
                    print(f"❌ Document upload also failed: {str(e2)}")
                    await reply.edit_text(f"⚠️ Upload failed: {str(e2)}")
                    return None
            finally:
                # ✅ Cleanup
                try:
                    if os.path.exists(filename):
                        os.remove(filename)
                except Exception as e:
                    print(f"⚠️ Could not remove file {filename}: {str(e)}")
                await reply.delete(True)
                await reply1.delete(True)

        else:
            # ⚠️ Notify about splitting
            notify_split = await m.reply_text(
                f"⚠️ The video is larger than 2GB ({human_readable_size(os.path.getsize(filename))})\n"
                f"⏳ Splitting into parts before upload..."
            )

            parts = split_large_video(filename)

            try:
                first_part_message = None
                for idx, part in enumerate(parts):
                    # Check if part file exists and is not empty
                    if not os.path.exists(part) or os.path.getsize(part) == 0:
                        print(f"⚠️ Part file missing or empty: {part}")
                        continue
                        
                    part_dur = int(get_duration(part))
                    part_num = idx + 1
                    total_parts = len(parts)
                    part_caption = f"{cc}\n\n📦 Part {part_num} of {total_parts}"
                    part_filename = f"{name}_Part{part_num}.mp4"

                    upload_msg = await m.reply_text(f"📤 Uploading Part {part_num}/{total_parts}...")

                    try:
                        msg_obj = await bot.send_video(
                            chat_id=channel_id,
                            video=part,
                            caption=part_caption,
                            file_name=part_filename,
                            supports_streaming=True,
                            height=720,
                            width=1280,
                            thumb=thumbnail,
                            duration=part_dur,
                            progress=progress_bar,
                            progress_args=(upload_msg, time.time())
                        )
                        if first_part_message is None:
                            first_part_message = msg_obj
                    except Exception:
                        msg_obj = await bot.send_document(
                            chat_id=channel_id,
                            document=part,
                            caption=part_caption,
                            file_name=part_filename,
                            progress=progress_bar,
                            progress_args=(upload_msg, time.time())
                        )
                        if first_part_message is None:
                            first_part_message = msg_obj
                    
                    await upload_msg.delete(True)
                    # Try to remove the part file after upload
                    try:
                        if os.path.exists(part):
                            os.remove(part)
                    except Exception as e:
                        print(f"⚠️ Could not remove part file {part}: {str(e)}")

            except Exception as e:
                raise Exception(f"Upload failed at part {idx + 1}: {str(e)}")

            # ✅ Final messages
            if len(parts) > 1:
                await m.reply_text("✅ Large video successfully uploaded in multiple parts!")

            # Cleanup after split
            await reply.delete(True)
            await reply1.delete(True)
            if notify_split:
                await notify_split.delete(True)
            # Original file should already be removed in the parts loop
            # Just double-check it's gone
            try:
                if os.path.exists(filename):
                    os.remove(filename)
            except Exception as e:
                print(f"⚠️ Could not remove original file {filename}: {str(e)}")

            # Return first sent part message
            sent_message = first_part_message

        # 🧹 Cleanup generated thumbnail if applicable
        if thumb in ["/d", "no"] and temp_thumb and os.path.exists(temp_thumb):
            try:
                os.remove(temp_thumb)
            except Exception as e:
                print(f"⚠️ Could not remove temporary thumbnail: {str(e)}")
        
        return sent_message

    except Exception as err:
        # Try to clean up any temporary files even if there was an error
        try:
            if thumb in ["/d", "no"] and temp_thumb and os.path.exists(temp_thumb):
                os.remove(temp_thumb)
        except:
            pass  # Ignore cleanup errors during exception handling
        
        raise Exception(f"send_vid failed: {err}")
