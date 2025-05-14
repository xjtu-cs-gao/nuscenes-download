import os
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import tarfile
import time

files = {
    'v1.0-trainval01_blobs.tgz': ['https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval01_blobs.tgz', './v1.0-trainval01_blobs.tgz', 'cbf32d2ea6996fc599b32f724e7ce8f2'],
    'v1.0-trainval02_blobs.tgz': ['https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval02_blobs.tgz', './v1.0-trainval02_blobs.tgz', 'aeecea4878ec3831d316b382bb2f72da'],
    'v1.0-trainval03_blobs.tgz': ['https://d36yt3mvayqw5m.cloudfront.net/public/v1.0/v1.0-trainval03_blobs.tgz', './v1.0-trainval03_blobs.tgz', '595c29528351060f94c935e3aaf7b995'],
    'v1.0-trainval04_blobs.tgz': ['https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval04_blobs.tgz', './v1.0-trainval04_blobs.tgz', 'b55eae9b4aa786b478858a3fc92fb72d'],
    'v1.0-trainval05_blobs.tgz': ['https://d36yt3mvayqw5m.cloudfront.net/public/v1.0/v1.0-trainval05_blobs.tgz', './v1.0-trainval05_blobs.tgz', '1c815ed607a11be7446dcd4ba0e71ed0'],
    'v1.0-trainval06_blobs.tgz': ['https://d36yt3mvayqw5m.cloudfront.net/public/v1.0/v1.0-trainval06_blobs.tgz', './v1.0-trainval06_blobs.tgz', '7273eeea36e712be290472859063a678'],
    'v1.0-trainval07_blobs.tgz': ['https://d36yt3mvayqw5m.cloudfront.net/public/v1.0/v1.0-trainval07_blobs.tgz', './v1.0-trainval07_blobs.tgz', '46674d2b2b852b7a857d2c9a87fc755f'],
    'v1.0-trainval08_blobs.tgz': ['https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval08_blobs.tgz', './v1.0-trainval08_blobs.tgz', '37524bd4edee2ab99678909334313adf'],
    'v1.0-trainval09_blobs.tgz': ['https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval09_blobs.tgz', './v1.0-trainval09_blobs.tgz', 'a7fcd6d9c0934e4052005aa0b84615c0'],
    'v1.0-trainval10_blobs.tgz': ['https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval10_blobs.tgz', './v1.0-trainval10_blobs.tgz', '31e795f2c13f62533c727119b822d739'],
    'v1.0-trainval_meta.tgz': ['https://d36yt3mvayqw5m.cloudfront.net/public/v1.0/v1.0-trainval_meta.tgz', './v1.0-trainval_meta.tgz', '537d3954ec34e5bcb89a35d4f6fb0d4a'],
}

def md5sum(filename, buf_size=1024*1024):
    md5 = hashlib.md5()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()

def extract_tar(filepath, position):
    try:
        # 获取文件所在目录
        extract_dir = os.path.dirname(filepath) or '.'
        
        with tarfile.open(filepath, 'r:*') as tar:
            # 解压到当前目录
            tar.extractall(path=extract_dir)
        tqdm.write(f"[{os.path.basename(filepath)}] 解压完成。")
        return True
    except Exception as e:
        tqdm.write(f"[{os.path.basename(filepath)}] 解压失败：{e}")
        return False

def download_file(name, url, out_path, md5, position):
    # 检查文件是否已存在并且MD5正确
    if os.path.exists(out_path):
        file_md5 = md5sum(out_path)
        if file_md5 == md5:
            tqdm.write(f"[{name}] 文件已存在且MD5校验通过，跳过下载，开始解压...")
            extract_tar(out_path, position)
            return
        else:
            tqdm.write(f"[{name}] 文件已存在但MD5不匹配，将继续下载...")
    
    # 确定文件大小和断点续传的起始位置
    file_size = 0
    headers = {}
    if os.path.exists(out_path):
        file_size = os.path.getsize(out_path)
        headers['Range'] = f'bytes={file_size}-'
        tqdm.write(f"[{name}] 从 {file_size} 字节处继续下载")
    
    try:
        with requests.get(url, headers=headers, stream=True, timeout=60) as r:
            # 如果服务器不支持断点续传，重新下载
            if r.status_code == 416:  # 请求范围不满足
                tqdm.write(f"[{name}] 服务器不支持断点续传或文件大小已变更，重新下载")
                file_size = 0
                headers = {}
                with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    total = int(r.headers.get('content-length', 0))
                    mode = 'wb'  # 重新写入
            else:
                r.raise_for_status()
                # 获取内容长度
                if 'content-length' in r.headers:
                    total = int(r.headers.get('content-length', 0)) + file_size
                else:
                    total = 0  # 未知大小
                mode = 'ab'  # 追加模式
            
            # 创建进度条
            with open(out_path, mode) as f, tqdm(
                desc=f"{name}",
                initial=file_size,
                total=total,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                position=position,
                leave=True,
                miniters=1,
                ascii=True,
                dynamic_ncols=True,
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
            
            # 校验MD5
            file_md5 = md5sum(out_path)
            if file_md5 != md5:
                tqdm.write(f"[{name}] 错误: MD5校验失败！期望:{md5} 实际:{file_md5}")
            else:
                tqdm.write(f"[{name}] 下载并校验成功，开始解压...")
                extract_tar(out_path, position)
    except requests.exceptions.RequestException as e:
        tqdm.write(f"[{name}] 下载出错: {e}，5秒后重试...")
        time.sleep(5)
        download_file(name, url, out_path, md5, position)
    except Exception as e:
        tqdm.write(f"[{name}] 错误: {e}")

def main():
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for idx, (name, (url, out_path, md5)) in enumerate(files.items()):
            futures.append(executor.submit(download_file, name, url, out_path, md5, idx))
        for future in as_completed(futures):
            future.result()

if __name__ == '__main__':
    # 需要先安装 tqdm: pip install tqdm
    main()
