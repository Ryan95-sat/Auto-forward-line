import json
import os
from datetime import datetime

from loguru import logger
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
    logger.info(f"info: {info}")
    try:
        text = info["events"][0]["message"]["text"]
        logger.info(f'text: {text}')
        timestamp = int(info["events"][0]["timestamp"] / 1000)
        date = datetime.strftime(datetime.fromtimestamp(timestamp), '%Y-%m-%d %H:%M:%S')
        logger.info(f'date: {date}')
        user_id = info["events"][0]["source"]["userId"]
        group_id = info["events"][0]["source"].get("groupId", "")
        logger.info(f'userId: {user_id}')
        logger.info(f'groupId: {group_id}')
        if text:
            value = (user_id, text, date, group_id)
            mysql_client.insert_one('''insert ignore into line_dialogue (user_id, message, date, group_id) values
                        (%s, %s, %s, %s)
            ''', value=value)
    except Exception as e:
        logger.info(e)
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
    logger.info(f"event: {event}")
    global working_status
    if event.message.type != "text":
        return

    text = event.message.text
    if text.startswith('@Fl Movie'):
        working_status = True
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="您好，有什么可以帮助您的吗？ ^_^ "))
        return
    # if event.message.text == "你好":
    #     working_status = True
    #     line_bot_api.reply_message(
    #         event.reply_token,
    #         TextSendMessage(text="您好，有什么可以帮助您的吗？ ^_^ "))
    #     return
    #
    # if event.message.text == "再见":
    #     working_status = False
    #     line_bot_api.reply_message(
    #         event.reply_token,
    #         TextSendMessage(text="好的，我乖乖閉嘴 > <，如果想要我繼續說話，請跟我說 「說話」 > <"))
    #     return
    #
    # if working_status:
    #     line_bot_api.reply_message(
    #         event.reply_token,
    #         TextSendMessage(text="test test"))


def parse_data(data):
    try:
        for event in data['events']:
            # ori_data = json.dumps(event, ensure_ascii=False)
            text = event["message"]["text"]
            logger.info(f'text: {text}')
            timestamp = int(event["timestamp"] / 1000)
            date = datetime.strftime(datetime.fromtimestamp(timestamp), '%Y-%m-%d %H:%M:%S')
            logger.info(f'date: {date}')
            user_id = event["source"]["userId"]
            logger.info(f'userId: {user_id}')
            group_id = event["source"].get("groupId", "")
            logger.info(f'groupId: {group_id}')
            value = [user_id, text, date, group_id]
            mysql_client.insert_one('''insert ignore into line_dialogue (user_id, message, date, group_id) values
                                    (%s, %s, %s, %s)
                        ''', value=value)
            logger.info(f"Success save {user_id}.")
    except Exception as e:
        logger.info(e)


if __name__ == "__main__":
    app.run('0.0.0.0', port=8090)
    # gunicron

