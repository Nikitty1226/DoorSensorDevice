from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import RPi.GPIO as GPIO

import threading
import json
import os
import logging
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv


# 環境変数読み込み
load_dotenv()

try:
    client = os.getenv("CLIENT")
    endpoint_url = os.getenv("ENDPOINT_URL")
    root_ca_path = os.getenv("AWS_ROOT_CA_PATH")
    private_key_path = os.getenv("AWS_PRIVATE_KEY_PATH")
    certificate_path = os.getenv("AWS_CERTIFICATE_PATH")
    sensor_topic = os.getenv("SENSOR_TOPIC")
    publish_interval = int(os.getenv("PUBLISH_INTERVAL"))
    reconnection_interval = int(os.getenv("RECONNECTION_INTERVAL"))
    gpio_pin = int(os.getenv("GPIO_PIN"))
except Exception as e:
    logging.error(f"環境変数設定エラー: {e}")
    exit(1)   

# ログ設定
logging.basicConfig(
    filename="logfile.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger('AWSIoTPythonSDK').setLevel(logging.ERROR)

# MQTTClient初期化
myMQTTClient = AWSIoTMQTTClient(client)
myMQTTClient.configureEndpoint(endpoint_url, 443)
myMQTTClient.configureCredentials(root_ca_path, private_key_path, certificate_path)

# GPIOの設定
try:
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(gpio_pin, GPIO.IN, pull_up_down = GPIO.PUD_UP)
except Exception as e:
    logging.error(f"GPIO設定エラー: {e}")
    exit(1)

# JSTの設定
jst = timezone(timedelta(hours=9))

# プログラムの開始通知
logging.info("通知：プログラム開始")

def connect_to_aws():
    """AWS IoT Core への接続"""
    while True:
        try:
            myMQTTClient.connect()
            logging.info("通知：AWS接続成功")
            break
        except Exception as e:
            logging.error(f"AWS接続失敗通知：{e}")
            time.sleep(reconnection_interval)

def publish_message():
    """AWSIoTCoreへメッセージを送信"""
    # GPIOの初期状態の入力
    past_value = GPIO.input(gpio_pin)

    while True:
        value = GPIO.input(gpio_pin)
        nowtime = datetime.now(jst).replace(microsecond=0)

        if value != past_value:
            if value == 1: #扉が開いたとき
                msgjson = {
                    "timestamp": f"{nowtime}",
                    "status": "open"
                }
                try:
                    myMQTTClient.publish(sensor_topic, json.dumps(msgjson), 1)
                    logging.info(f"通知：センサデータ送信成功 topic:{sensor_topic}, message:{msgjson}")
                except Exception as e:
                    logging.error(f"AWSパブリッシュエラー通知：{e}")
                    logging.info("通知：AWSとの再接続を試みます")
                    connect_to_aws()

        past_value = value
        time.sleep(1)

def send_heartbeat():
    """一定時間ごとに死活監視のメッセージを送信"""
    while True:
        nowtime = datetime.now(jst).replace(microsecond=0)
        msgjson = {
            "timestamp": f"{nowtime}",
            "status": "heartbeat"
        }
        try:
            myMQTTClient.publish(sensor_topic, json.dumps(msgjson), 1)
            logging.info(f"通知：ハートビート送信成功 topic:{sensor_topic}, message:{msgjson}")
        except Exception as e:
            logging.error(f"ハートビート送信失敗通知: {e}")
            logging.info("通知：AWSとの再接続を試みます")
            connect_to_aws()
        
        time.sleep(publish_interval)

def main():
    """メイン処理"""
    try:
        connect_to_aws()
        event_thread = threading.Thread(target=publish_message, daemon=True)
        heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
        event_thread.start()
        heartbeat_thread.start()
        event_thread.join()
        heartbeat_thread.join()

    except KeyboardInterrupt:
        logging.info("通知：手動停止（Control+C）を検知")

    except Exception as e:
        logging.error(f"エラー通知：{e}")

    finally:
        logging.info("通知：プログラム終了")
        myMQTTClient.disconnect()
        GPIO.cleanup()

if __name__ == "__main__":
    main()