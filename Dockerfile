FROM python:3.9-slim
RUN pip install flask
RUN mkdir /live_archive
# 创建程序工作目录
WORKDIR /app
COPY move_when_file_closed.py .
CMD ["python", "move_when_file_closed.py"]