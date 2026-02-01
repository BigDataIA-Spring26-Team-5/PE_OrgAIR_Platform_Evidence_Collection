"""
Infrastructure Test
PE Org-AI-R Platform

Checks:
- Snowflake connection
- Redis connection (Docker-based)
- AWS S3 bucket access

Run using:
    python -m app.scripts.test_connections
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime


def load_env():
    """Load .env from project root."""
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    load_dotenv(dotenv_path=env_path)


def test_snowflake():
    print("\n🔹 Testing Snowflake connection...")
    try:
        from app.services.snowflake import get_snowflake_connection
        print("Loaded SECRET_KEY:", bool(os.getenv("SECRET_KEY")))
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
        user, role = cur.fetchone()
        cur.close()
        conn.close()

        print(f"✅ Snowflake connected (User: {user}, Role: {role})")
        return True

    except Exception as e:
        import traceback
        print("❌ Snowflake connection failed")
        traceback.print_exc()   # 👈 THIS is the key line
        return False


def test_redis():
    print("\n🔹 Testing Redis connection (Docker)...")
    try:
        import redis

        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        redis_db = int(os.getenv("REDIS_DB", 0))

        client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            socket_connect_timeout=5,
            decode_responses=True,
        )

        client.ping()
        client.set("infra_test_key", "ok", ex=10)
        value = client.get("infra_test_key")

        if value == "ok":
            print(f"✅ Redis connected ({redis_host}:{redis_port}, db={redis_db})")

        client.close()
        return True

    except Exception as e:
        print("❌ Redis connection failed")
        print(str(e))
        return False


def test_s3():
    print("\n🔹 Testing AWS S3 connection...")
    try:
        import boto3
        from botocore.exceptions import ClientError

        bucket = os.getenv("S3_BUCKET", "pe-orgair-platform")
        region = os.getenv("AWS_REGION")

        if not region:
            raise ValueError("AWS_REGION not set")

        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=region,
        )

        s3.head_bucket(Bucket=bucket)
        print(f"✅ S3 access successful (Bucket: {bucket}, Region: {region})")
        return True

    except ClientError as e:
        print("❌ S3 access failed")
        print(e.response["Error"]["Code"], e.response["Error"]["Message"])
        return False

    except Exception as e:
        print("❌ S3 access failed")
        print(str(e))
        return False


def main():
    print("🚀 PE Org-AI-R Platform – Infrastructure Smoke Test")
    print(f"🕒 Timestamp: {datetime.utcnow().isoformat()}Z")

    load_env()

    results = {
        "snowflake": test_snowflake(),
        "redis": test_redis(),
        "s3": test_s3(),
    }

    print("\n📊 Test Summary")
    for service, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"- {service.capitalize():10s}: {status}")

    if all(results.values()):
        print("\n🎉 All infrastructure checks passed!")
    else:
        print("\n⚠️ Some infrastructure checks failed. See logs above.")


if __name__ == "__main__":
    main()

# how to run this test file
# python -m app.scripts.test_connections