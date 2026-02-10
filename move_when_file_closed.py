import os
from pathlib import Path
import shutil
import logging
import sys
import threading
from flask import Flask, request, jsonify

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)

# --- 配置区 ---
SOURCE_BASE = "/rec"
TARGET_BASE = "/live_archive"

# 补全逻辑：检查变量是否为空
if not SOURCE_BASE or not TARGET_BASE:
    logging.error("错误: 未配置环境变量 REC_SOURCE_DIR 或 REC_TARGET_DIR")
    logging.error("请在 docker-compose.yml 的 environment 部分进行设置")
    sys.exit(1)  # 异常退出，Docker 会根据 restart 策略尝试重启

# 检查路径在容器内是否真实存在（可选，但非常有帮助）
if not os.path.exists(SOURCE_BASE):
    logging.warning(f"警告: 源路径 {SOURCE_BASE} 在容器内不存在，请检查 volumes 挂载")
    sys.exit(1)  # 异常退出，Docker 会根据 restart 策略尝试重启
if not os.path.exists(TARGET_BASE):
    logging.warning(f"警告: 目标路径 {TARGET_BASE} 在容器内不存在，请检查 volumes 挂载")
    sys.exit(1)  # 异常退出，Docker 会根据 restart 策略尝试重启

logging.info(f"配置加载成功: 源={SOURCE_BASE}, 目标={TARGET_BASE}")


def cleanup_empty_dirs(root_path):
    """递归清理空文件夹"""
    # topdown=False 是关键：先处理子目录，再处理父目录
    for root, dirs, files in os.walk(root_path, topdown=False):
        for name in dirs:
            dir_path = os.path.join(root, name)
            # 如果文件夹为空（没有文件也没有子文件夹）
            if not os.listdir(dir_path):
                try:
                    os.rmdir(dir_path)
                    logging.info(f"[清理] 成功删除空文件夹: {dir_path}")
                except Exception as e:
                    logging.error(f"[清理] 删除失败 {dir_path}: {e}")


def async_move_file(rel_path):
    """异步执行移动操作的函数"""
    p = Path(rel_path)
    src_file_path = Path(SOURCE_BASE) / p
    stem = src_file_path.stem
    src_dir = src_file_path.parent
    dst_dir = Path(TARGET_BASE) / p.parent

    try:
        dst_dir.mkdir(parents=True, exist_ok=True)

        files_moved_count = 0
        for f_name in os.listdir(src_dir):
            # 匹配逻辑：文件名包含 stem 且不是正在录制的临时文件 (.part)
            # if stem in f_name and not f_name.endswith(".part"):
            if stem in f_name:
                s_path = src_dir / f_name
                d_path = dst_dir / f_name

                try:
                    # 使用 shutil.move 处理跨文件系统移动
                    shutil.move(str(s_path), str(d_path))
                    logging.info(f"  -> 成功: {f_name}")
                    files_moved_count += 1
                except Exception as move_error:
                    logging.error(f"  -> 失败: {f_name}, 错误: {move_error}")

        logging.info(f"[任务结束] 关联文件处理完毕，共移动 {files_moved_count} 个文件")

        # --- 移动成功后，触发清理 ---
        cleanup_empty_dirs(SOURCE_BASE)
    except Exception as e:
        logging.error(f"异步移动异常 ({rel_path}): {str(e)}")


@app.route("/webhook", methods=["POST"])
def handle_webhook():
    data = request.json
    event = data.get("EventType")

    if event == "FileClosed":
        event_data = data.get("EventData", {})
        rel_path = event_data.get("RelativePath")

        if rel_path:
            # 创建并启动线程，不阻塞主流程
            thread = threading.Thread(target=async_move_file, args=(rel_path,))
            thread.start()

            logging.info(f"已将 {rel_path} 加入移动任务队列")
            return jsonify({"status": "task_started", "file": rel_path}), 202

    return jsonify({"status": "ignored"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
