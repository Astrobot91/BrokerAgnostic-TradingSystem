import os
import requests
from dotenv import load_dotenv

load_dotenv()

# bot = 't.me/plus91momentumbot'
def message(chat_id, message):
    payload = {'chat_id': chat_id, 'text': message}
    apiURL = f"https://api.telegram.org/bot{os.getenv('TELEGRAM_BOT_TOKEN')}/sendMessage?chat_id={chat_id}&text={message}"
    requests.post(apiURL, data=payload)

def broadcast_signal(signal):
    chat_ids = ['1134875199', '551406120']
    for chat_id in chat_ids:
        message(chat_id, signal)

