FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY forex_bot.py .

# subscribers.json и daily_state.json будут создаваться и жить здесь;
# на Railway стоит подключить persistent volume на /app, иначе при каждом
# передеплое подписчики и состояние дня теряются.
CMD ["python", "forex_bot.py"]
