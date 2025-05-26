import os
import subprocess

DATASET_DIR = "../dataset/bronze"
HDFS_TARGET_DIR = "/bronze"

def upload_to_hdfs(local_path, hdfs_path):
    # Buat folder di HDFS jika belum ada
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_path], check=True)
    # Upload file ke HDFS
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)

def main():
    for root, dirs, files in os.walk(DATASET_DIR):
        for file in files:
            local_file = os.path.join(root, file)
            # Path relatif dari dataset/bronze
            rel_dir = os.path.relpath(root, DATASET_DIR)
            # Simpan ke HDFS dengan struktur yang sama
            hdfs_dir = os.path.join(HDFS_TARGET_DIR, rel_dir).replace("\\", "/")
            print(f"Uploading {local_file} to {hdfs_dir} ...")
            upload_to_hdfs(local_file, hdfs_dir)

if __name__ == "__main__":
    main()