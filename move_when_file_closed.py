import os
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
    src_path = os.path.join(SOURCE_BASE, rel_path)
    dst_path = os.path.join(TARGET_BASE, rel_path)

    try:
        if not os.path.exists(src_path):
            logging.error(f"文件不存在，跳过: {src_path}")
            return

        # 确保目标文件夹存在
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)

        logging.info(f"开始异步移动: {rel_path}")
        # 跨分区移动：实质上是 cp + rm
        shutil.move(src_path, dst_path)
        logging.info(f"移动完成: {rel_path}")

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
