import os
import base64
import hashlib
import hmac
from datetime import datetime

from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

from tools.mysql import MysqlPool
from tools.settings import LINE_CHANNEL_SECRET, LINE_CHANNEL_ACCESS_TOKEN

mysql_client = MysqlPool()

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
line_handler = WebhookHandler(LINE_CHANNEL_SECRET)
working_status = os.getenv("DEFAULT_TALKING", default="true").lower() == "true"

app = Flask(__name__)


# domain root
@app.route('/')
def home():
    return 'Hello, World!'


@app.route("/webhook", methods=['POST'])
def callback():
    # get request body as text
    info = request.get_json()
    print(f'info: {info}')
    try:
        text = info["events"][0]["message"]["text"]
        print(f'text: {text}')
        timestamp = int(info["events"][0]["timestamp"] / 1000)
        date = datetime.strftime(datetime.fromtimestamp(timestamp), '%Y-%m-%d %H:%M:%S')
        print(f'date: {date}')
        user_id = info["events"][0]["source"]["userId"]
        group_id = info["events"][0]["source"].get("groupId", "")
        print(f'userId: {user_id}')
        print(f'groupId: {group_id}')
        if text:
            value = (user_id, text, date, group_id)
            print(value)
            mysql_client.insert_one('''insert ignore into line_dialogue (user_id, message, date, group_id) values
                        (%s, %s, %s, %s)
            ''', value=value)
    except Exception as e:
        print(e)
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)
    # handle webhook body
    try:
        line_handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


@line_handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    global working_status
    print(f"event: {event}")
    print(f"message: {event.message}")
    print(f"working_status: {working_status}")
    if event.message.type != "text":
        return

    if event.message.text == "你好":
        working_status = True
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="您好，有什么可以帮助您的吗？ ^_^ "))
        return

    if event.message.text == "再见":
        working_status = False
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="好的，我乖乖閉嘴 > <，如果想要我繼續說話，請跟我說 「說話」 > <"))
        return

    if working_status:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="test test"))


if __name__ == "__main__":
    app.run('0.0.0.0', port=8090)

