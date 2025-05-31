from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import RPi.GPIO as GPIO

import threading
import json
import os
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv


# 環境変数読み込み
load_dotenv()

client = os.getenv("CLIENT")
endpoint_url = os.getenv("ENDPOINT_URL")
root_ca_path = os.getenv("AWS_ROOT_CA_PATH")
private_key_path = os.getenv("AWS_PRIVATE_KEY_PATH")
certificate_path = os.getenv("AWS_CERTIFICATE_PATH")
sensor_topic = os.getenv("SENSOR_TOPIC")
interval = int(os.getenv("INTERVAL"))
reconnection_interval = int(os.getenv("RECONNECTION_INTERVAL"))
gpio_pin = int(os.getenv("GPIO_PIN"))

# ログ設定
logging.basicConfig(
    filename="logfile.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logging.getLogger('AWSIoTPythonSDK').setLevel(logging.CRITICAL)

# 初期化
myMQTTClient = AWSIoTMQTTClient(client)

# MQTTクライアントの設定
myMQTTClient.configureEndpoint(endpoint_url, 443)
myMQTTClient.configureCredentials(root_ca_path, private_key_path, certificate_path)
myMQTTClient.configureOfflinePublishQueueing(-1) #オフライン中に送信できなかったメッセージをキューにためる
myMQTTClient.configureDrainingFrequency(2) #再接続後にためていたメッセージを1秒間に何回送信するか
myMQTTClient.configureConnectDisconnectTimeout(10) #AWSIoTCoreへの接続・切断の時間制限(秒)を設定
myMQTTClient.configureMQTTOperationTimeout(5) #メッセージ送信・受信（QoS1の場合）にかかる時間の制限を設定

# GPIOの設定
try:
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(gpio_pin, GPIO.IN, pull_up_down = GPIO.PUD_UP)
except Exception as e:
    logging.error(f"GPIO設定エラー: {e}")
    exit(1)

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

    # GPIOと時刻の初期状態の入力
    past_value = GPIO.input(gpio_pin)

    while True:
        value = GPIO.input(gpio_pin)
        nowtime = datetime.now().replace(microsecond=0)

        if value != past_value:
            if value == 1: #扉が開いたとき
                msgjson = {
                    "timestamp": f"{nowtime}",
                    "sensor_value": value
                }
                try:
                    myMQTTClient.publish(sensor_topic, json.dumps(msgjson), 1)
                    logging.info(f"センサデータ送信成功通知：{msgjson}")
                except Exception as e:
                    logging.error(f"AWSパブリッシュエラー通知：{e}")
                    logging.info("通知：AWSとの再接続を試みます")
                    connect_to_aws()

        past_value = value
        time.sleep(1)

def send_heartbeat():
    """一定時間ごとに死活監視のメッセージを送信"""

    while True:
        nowtime = datetime.now().replace(microsecond=0)
        msgjson = {
            "timestamp": f"{nowtime}",
            "heart_beat": 1
        }
        try:
            myMQTTClient.publish(sensor_topic, json.dumps(msgjson), 1)
            logging.info(f"ハートビート送信成功通知: {msgjson}")
        except Exception as e:
            logging.error(f"ハートビート送信失敗通知: {e}")
            logging.info("通知：AWSとの再接続を試みます")
            connect_to_aws()
        
        time.sleep(interval)

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