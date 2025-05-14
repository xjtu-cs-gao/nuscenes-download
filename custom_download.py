import os
import hashlib
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import tarfile

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
        with tarfile.open(filepath, 'r:*') as tar:
            # 解压到当前目录
            tar.extractall(path=os.path.dirname(filepath))
        tqdm.write(f"[{os.path.basename(filepath)}] 解压完成。")
    except Exception as e:
        tqdm.write(f"[{os.path.basename(filepath)}] 解压失败：{e}")

def download_file(name, url, out_path, md5, position):
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total = int(r.headers.get('content-length', 0))
            desc = f"{name}"
            with open(out_path, 'wb') as f, tqdm(
                desc=desc,
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
    except Exception as e:
        tqdm.write(f"[{name}] 错误: 下载失败，原因: {e}")

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
