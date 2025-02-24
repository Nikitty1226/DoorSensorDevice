from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
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
topic = os.getenv("TOPIC")
interval = os.getenv("INTERVAL")

if interval:
    interval = float(interval)

if not all([client, endpoint_url, root_ca_path, private_key_path, certificate_path, topic, interval]):
    print("環境変数が設定されていません")
    exit(1)

# ログ設定
logging.basicConfig(
    filename="logfile.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logging.getLogger('AWSIoTPythonSDK').setLevel(logging.WARNING)  # より少ないログを表示

# 初期化
myMQTTClient = AWSIoTMQTTClient(client)

# MQTTクライアントの設定
myMQTTClient.configureEndpoint(endpoint_url, 443)
myMQTTClient.configureCredentials(root_ca_path, private_key_path, certificate_path)
myMQTTClient.configureOfflinePublishQueueing(-1) #オフライン中に送信できなかったメッセージをキューにためる
myMQTTClient.configureDrainingFrequency(2) #再接続後にためていたメッセージを1秒間に何回送信するか
myMQTTClient.configureConnectDisconnectTimeout(10) #AWSIoTCoreへの接続・切断の時間制限(秒)を設定
myMQTTClient.configureMQTTOperationTimeout(5) #メッセージ送信・受信（QoS1の場合）にかかる時間の制限を設定

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
            time.sleep(5)

def publish_message():
    """AWSIoTCoreへメッセージを送信"""
    while True:
        msgjson = {
            "test1": 1,
            "test2": 2
        }

        try:
            myMQTTClient.publish(topic, json.dumps(msgjson), 1)
            logging.info(f"AWSパブリッシュ成功通知：{msgjson}")

        except Exception as e:
            logging.error(f"AWSパブリッシュエラー通知：{e}")
            connect_to_aws()

        time.sleep(interval)

def main():
    """メイン処理"""
    try:
        connect_to_aws()
        publish_message()

    except KeyboardInterrupt:
        logging.info("通知：手動停止（Control+C）を検知")

    except Exception as e:
        logging.error(f"エラー通知：{e}")

    finally:
        logging.info("通知：プログラム終了")
        myMQTTClient.disconnect()
        exit(1)

if __name__ == "__main__":
    main()