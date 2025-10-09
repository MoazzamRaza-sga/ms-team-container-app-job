FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY attendance_reports_main.py .
RUN pip install requests
CMD ["python", "attendance_reports_main.py"]