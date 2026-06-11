# Имадж с легковесным Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Сначала копируем зависимости для кэширования слоев Docker
COPY requirements.txt .

# Устанавливаем библиотеки без сохранения кэша (для уменьшения размера)
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект (папки src, utils, database, config и main.py)
COPY . .

# Команда, которая автоматически запустится при старте контейнера
CMD ["python", "main.py"]
