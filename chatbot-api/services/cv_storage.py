"""
@file cv_storage.py
@brief CV file storage service using MinIO (object store) + Iceberg (metadata).

Architecture:
  - PDF files  → MinIO bucket "user-cvs" at path {user_uid}/{cv_id}/{filename}
  - Metadata   → Iceberg table iceberg.app_db.user_cvs (queried via Trino)

This keeps all user data within the system's own data lake infrastructure
instead of relying on external cloud storage.
"""

import logging
import uuid
from io import BytesIO

import trino
from minio import Minio
from minio.error import S3Error

from config import settings

logger = logging.getLogger(__name__)

_TABLE = "iceberg.app_db.user_cvs"


def _minio_client() -> Minio:
    """
    @brief Create and return a configured MinIO client from application settings.

    @return  Minio client instance (no TLS, credentials from settings).
    """
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=False,
    )


def _trino_conn() -> trino.dbapi.Connection:
    """
    @brief Open and return a Trino DBAPI connection for metadata operations.

    @return  Open Trino connection targeting the iceberg catalog.
    """
    return trino.dbapi.connect(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog="iceberg",
    )


def _ensure_bucket(client: Minio) -> None:
    """
    @brief Create the CV bucket in MinIO if it does not already exist.

    @param client  Active Minio client instance.
    """
    if not client.bucket_exists(settings.minio_cv_bucket):
        client.make_bucket(settings.minio_cv_bucket)
        logger.info("Created MinIO bucket: %s", settings.minio_cv_bucket)


def upload_cv(user_uid: str, filename: str, data: bytes) -> dict:
    """
    @brief Upload a CV PDF to MinIO and persist its metadata to the Iceberg table.

    @param user_uid  Unique user identifier (used as the path prefix in MinIO).
    @param filename  Original filename of the uploaded PDF.
    @param data      Raw PDF bytes.
    @return          Dict with cv_id, filename, minio_path, and file_size.
    """
    cv_id      = str(uuid.uuid4())
    minio_path = f"{user_uid}/{cv_id}/{filename}"

    client = _minio_client()
    _ensure_bucket(client)
    client.put_object(
        settings.minio_cv_bucket,
        minio_path,
        BytesIO(data),
        len(data),
        content_type="application/pdf",
    )
    logger.info("CV uploaded to MinIO: %s", minio_path)

    safe_filename = filename.replace("'", "''")
    safe_path     = minio_path.replace("'", "''")
    sql = f"""
        INSERT INTO {_TABLE} (cv_id, user_uid, filename, file_size, minio_path, uploaded_at)
        VALUES (
            '{cv_id}',
            '{user_uid}',
            '{safe_filename}',
            {len(data)},
            '{safe_path}',
            CURRENT_TIMESTAMP
        )
    """
    conn = _trino_conn()
    try:
        conn.cursor().execute(sql)
        logger.info("CV metadata saved to Iceberg: cv_id=%s", cv_id)
    finally:
        conn.close()

    return {"cv_id": cv_id, "filename": filename, "minio_path": minio_path, "file_size": len(data)}


def list_cvs(user_uid: str) -> list[dict]:
    """
    @brief Return all CVs for a user, ordered newest first.

    @param user_uid  Unique user identifier.
    @return          List of metadata dicts (cv_id, filename, file_size, minio_path, uploaded_at).
    """
    sql = f"""
        SELECT cv_id, filename, file_size, minio_path, uploaded_at
        FROM {_TABLE}
        WHERE user_uid = '{user_uid}'
        ORDER BY uploaded_at DESC
    """
    conn = _trino_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def get_cv_bytes(user_uid: str, cv_id: str) -> tuple[bytes, str]:
    """
    @brief Download a CV PDF from MinIO and return its raw bytes and filename.

    @param user_uid  Unique user identifier.
    @param cv_id     UUID of the CV to retrieve.
    @return          Tuple of (pdf_bytes, filename).
    @throws ValueError  If no matching CV is found in the metadata table.
    """
    sql = f"""
        SELECT filename, minio_path FROM {_TABLE}
        WHERE cv_id = '{cv_id}' AND user_uid = '{user_uid}'
    """
    conn = _trino_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
    finally:
        conn.close()

    if not row:
        raise ValueError(f"CV not found: cv_id={cv_id}")

    filename, minio_path = row
    client = _minio_client()
    response = client.get_object(settings.minio_cv_bucket, minio_path)
    data = response.read()
    response.close()
    return data, filename


def delete_cv(user_uid: str, cv_id: str) -> None:
    """
    @brief Delete a CV's PDF from MinIO and remove its metadata row from Iceberg.

    @param user_uid  Unique user identifier.
    @param cv_id     UUID of the CV to delete.
    @throws ValueError  If no matching CV is found in the metadata table.
    """
    sql = f"""
        SELECT minio_path FROM {_TABLE}
        WHERE cv_id = '{cv_id}' AND user_uid = '{user_uid}'
    """
    conn = _trino_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"CV not found: cv_id={cv_id}")
        minio_path = row[0]

        cursor.execute(f"""
            DELETE FROM {_TABLE}
            WHERE cv_id = '{cv_id}' AND user_uid = '{user_uid}'
        """)
        logger.info("CV metadata deleted from Iceberg: cv_id=%s", cv_id)
    finally:
        conn.close()

    try:
        _minio_client().remove_object(settings.minio_cv_bucket, minio_path)
        logger.info("CV file deleted from MinIO: %s", minio_path)
    except S3Error as e:
        logger.warning("MinIO delete failed (file may not exist): %s", e)
