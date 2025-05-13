#!/usr/bin/env python3
"""
Hypha Database Recovery Script (with additional S3 sources)

This script rebuilds a Hypha SQLite database from S3 backups.
It scans S3 for all workspaces and artifacts, including collection artifacts in alternative S3 buckets.
"""

import os
import json
import asyncio
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime
from botocore.config import Config
from sqlalchemy import Column, JSON, UniqueConstraint, select, text, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import Field, SQLModel
from aiobotocore.session import get_session


SQLModel.metadata.clear()

# --- Config ---
ENV_FILE = ".env"
DATABASE_PATH = "./hypha-app-database.db"
LOGLEVEL = "DEBUG"
S3_BUCKET = "hypha-workspaces"
ADDITIONAL_ENV_FILES = ["bioimageio_env", "shareloc_env"]
ADDITIONAL_ENV_KEYS = [
    "SANDBOX_ZENODO_ACCESS_TOKEN",
    "ZENODO_ACCESS_TOKEN",
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_REGION_NAME",
    "S3_PREFIX",
    "S3_BUCKET",
]
DATABASE_URI = "sqlite+aiosqlite:///./hypha-app-database.db"
engine = create_async_engine(DATABASE_URI, echo=False)
session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

DB_USER = "hypha-admin"

# PostgreSQL engine and session
pg_engine = create_async_engine(
    DATABASE_URI, echo=False
)  # Set echo to False to reduce output
pg_session_maker = async_sessionmaker(
    pg_engine, expire_on_commit=False, class_=AsyncSession
)

# --- Logging ---
logger = logging.getLogger("hypha_recovery")
logger.setLevel(getattr(logging, LOGLEVEL))
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)


# --- Load env ---
def load_env_file(filepath: str) -> dict:
    env = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                value = value.strip().strip('"').strip("'")
                env[key] = value
    return env


# Load main env
env_vars = load_env_file(ENV_FILE) if Path(ENV_FILE).exists() else {}

# Load additional S3 sources
ADDITIONAL_S3_SOURCES = []
for env_file in ADDITIONAL_ENV_FILES:
    if not Path(env_file).exists():
        logger.warning("Additional env file not found: %s", env_file)
        continue
    env = load_env_file(env_file)
    conf = {k: env.get(k, "") for k in ADDITIONAL_ENV_KEYS}
    if not conf["S3_BUCKET"] or not conf["S3_PREFIX"]:
        logger.warning("Skipping incomplete S3 config in %s", env_file)
        continue
    ADDITIONAL_S3_SOURCES.append(
        {
            "endpoint_url": conf["S3_ENDPOINT_URL"],
            "access_key_id": conf["S3_ACCESS_KEY_ID"],
            "secret_access_key": conf["S3_SECRET_ACCESS_KEY"],
            "region_name": conf.get("S3_REGION_NAME", "us-east-1"),
            "bucket": conf["S3_BUCKET"],
            "prefix": conf["S3_PREFIX"],
            "secrets": conf,
        }
    )

# --- S3 Config ---
S3_CONFIG = {
    "endpoint_url": env_vars.get("ENDPOINT_URL", os.environ.get("HYPHA_ENDPOINT_URL")),
    "access_key_id": env_vars.get(
        "ACCESS_KEY_ID", os.environ.get("HYPHA_ACCESS_KEY_ID")
    ),
    "secret_access_key": env_vars.get(
        "SECRET_ACCESS_KEY", os.environ.get("HYPHA_SECRET_ACCESS_KEY")
    ),
    "region_name": "us-east-1",
    "bucket": S3_BUCKET,
    "prefix": "artifacts",
}

DATABASE_URI = f"sqlite+aiosqlite:///{DATABASE_PATH}"
if os.path.exists(DATABASE_PATH):
    os.remove(DATABASE_PATH)
    logger.info("Removed existing database at %s", DATABASE_PATH)


# --- Model ---
class ArtifactModel(SQLModel, table=True):
    __tablename__ = "artifacts"
    id: str = Field(primary_key=True)
    type: Optional[str] = Field(default=None)
    workspace: str = Field(index=True)
    parent_id: Optional[str] = Field(default=None)
    alias: Optional[str] = Field(default=None)
    manifest: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    staging: Optional[list] = Field(default=None, sa_column=Column(JSON))
    download_count: float = Field(default=0.0)
    view_count: float = Field(default=0.0)
    file_count: int = Field(default=0)
    created_at: int = Field()
    created_by: Optional[str] = Field(default=None)
    last_modified: int = Field()
    versions: Optional[list] = Field(default=None, sa_column=Column(JSON))
    config: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    secrets: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    __table_args__ = (
        UniqueConstraint("workspace", "alias", name="_workspace_alias_uc"),
    )


# --- Utils ---
def safe_join(directory, *pathnames):
    result = directory
    for pathname in pathnames:
        result = result.rstrip("/") + "/" + pathname.lstrip("/")
    return result


async def create_s3_client(config):
    url = config["endpoint_url"]
    if not url.startswith("http"):
        url = "https://" + url
    return get_session().create_client(
        "s3",
        endpoint_url=url.rstrip("/"),
        aws_access_key_id=config["access_key_id"],
        aws_secret_access_key=config["secret_access_key"],
        region_name=config["region_name"],
        config=Config(connect_timeout=60, read_timeout=300),
    )


async def list_objects_async(s3_client, bucket, prefix=None, delimiter="/"):
    prefix = prefix or ""
    response = await s3_client.list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter=delimiter
    )

    def parse(resp):
        out = []
        for p in resp.get("CommonPrefixes", []):
            out.append(
                {
                    "name": p["Prefix"].rstrip(delimiter).split(delimiter)[-1],
                    "type": "directory",
                }
            )
        for obj in resp.get("Contents", []):
            if not obj["Key"].endswith("/"):
                out.append(
                    {
                        "name": obj["Key"].split("/")[-1],
                        "type": "file",
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"],
                    }
                )
        return out

    items = parse(response)
    while response.get("IsTruncated"):
        token = response["NextContinuationToken"]
        response = await s3_client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter=delimiter, ContinuationToken=token
        )
        items += parse(response)
    return items


async def init_db(engine):
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def scan_artifact_prefix(s3_config, prefix, infer_workspace=False):
    artifacts = {}
    total_dirs = 0
    skipped_dirs = 0
    loaded = 0
    failed = 0

    async with await create_s3_client(s3_config) as s3_client:
        logger.info("Scanning bucket: %s, prefix: %s", s3_config["bucket"], prefix)
        try:
            artifact_dirs = await list_objects_async(
                s3_client, s3_config["bucket"], prefix.rstrip("/") + "/", delimiter="/"
            )
        except Exception as e:
            logger.error("Failed to list prefix %s: %s", prefix, e)
            return artifacts

        total_dirs = len(artifact_dirs)
        logger.info("Found %d artifact directories under %s", total_dirs, prefix)

        for artifact_dir in artifact_dirs:
            if artifact_dir["type"] != "directory":
                skipped_dirs += 1
                continue

            artifact_id = artifact_dir["name"]
            version_prefix = safe_join(prefix.rstrip("/"), artifact_id)

            try:
                version_objects = await list_objects_async(
                    s3_client, s3_config["bucket"], version_prefix + "/"
                )
            except Exception as e:
                logger.warning(
                    "Failed to list versions for artifact %s in prefix %s: %s",
                    artifact_id,
                    version_prefix,
                    e,
                )
                failed += 1
                continue

            json_files = [
                obj
                for obj in version_objects
                if obj["type"] == "file" and obj["name"].endswith(".json")
            ]
            if not json_files:
                logger.debug(
                    "No JSON version files for artifact %s under %s",
                    artifact_id,
                    version_prefix,
                )
                continue

            json_files.sort(key=lambda x: x["last_modified"])
            latest_version_key = safe_join(version_prefix, json_files[-1]["name"])

            try:
                response = await s3_client.get_object(
                    Bucket=s3_config["bucket"], Key=latest_version_key
                )
                content = await response["Body"].read()
                data = json.loads(content)
                data.setdefault("id", artifact_id)
                if infer_workspace:
                    workspace = prefix.split("/")[0]
                    data.setdefault("workspace", workspace)
                else:
                    data.setdefault("workspace", "external")
                artifacts[artifact_id] = data
                loaded += 1
                logger.debug(
                    "Loaded artifact %s from %s", artifact_id, latest_version_key
                )
            except Exception as e:
                logger.warning(
                    "Failed to load artifact %s from %s: %s",
                    artifact_id,
                    latest_version_key,
                    e,
                )
                failed += 1

    logger.info(
        "Finished scanning prefix '%s': %d artifacts loaded, %d skipped, %d failed",
        prefix,
        loaded,
        skipped_dirs,
        failed,
    )
    return artifacts


async def rebuild_database():
    logger.info("Starting database recovery")
    engine = create_async_engine(DATABASE_URI, echo=False)
    await init_db(engine)
    session_maker = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    artifacts_by_id = {}

    # Main bucket: scan workspace folders
    async with await create_s3_client(S3_CONFIG) as s3_client:
        workspaces = await list_objects_async(s3_client, S3_CONFIG["bucket"])
        logger.info("Found %d top-level entries in main S3 bucket", len(workspaces))

        for ws in [
            w for w in workspaces if w["type"] == "directory" and w["name"] != "etc"
        ]:
            ws_name = ws["name"]
            artifacts_prefix = safe_join(ws_name, "artifacts")
            logger.info("Scanning workspace: %s", ws_name)
            artifacts = await scan_artifact_prefix(
                S3_CONFIG, artifacts_prefix, infer_workspace=True
            )
            logger.info("Workspace %s: %d artifacts found", ws_name, len(artifacts))
            artifacts_by_id.update(artifacts)

    # Additional buckets: use flat prefix
    for source in ADDITIONAL_S3_SOURCES:
        logger.info(
            "Scanning additional S3 source: %s/%s/%s",
            source["endpoint_url"],
            source["bucket"],
            source["prefix"],
        )
        artifacts = await scan_artifact_prefix(
            source, source["prefix"], infer_workspace=False
        )
        logger.info(
            "Source %s:%s: %d artifacts found",
            source["bucket"],
            source["prefix"],
            len(artifacts),
        )
        artifacts_by_id.update(artifacts)

    logger.info(
        "Total artifacts collected: %d. Inserting into database...",
        len(artifacts_by_id),
    )

    inserted = 0
    skipped = 0
    overwritten = 0
    failed = 0

    async with session_maker() as session:
        for artifact_id, artifact_data in artifacts_by_id.items():
            try:
                async with session.begin():
                    alias = artifact_data.get("alias")
                    ws = artifact_data.get("workspace")
                    if alias:
                        stmt = select(ArtifactModel).where(
                            ArtifactModel.workspace == ws, ArtifactModel.alias == alias
                        )
                        result = await session.execute(stmt)
                        existing_artifact = result.scalar_one_or_none()

                        if existing_artifact:
                            s3_time = artifact_data.get("last_modified", 0)
                            db_time = existing_artifact.last_modified or 0

                            if s3_time > db_time:
                                logger.warning(
                                    "Duplicate alias found for workspace='%s', alias='%s' — S3 artifact is NEWER (S3: %s, DB: %s). Overwriting.\n"
                                    "→ S3 created_at: %s, file_count: %s\n"
                                    "→ DB created_at: %s, file_count: %s",
                                    ws,
                                    alias,
                                    s3_time,
                                    db_time,
                                    artifact_data.get("created_at"),
                                    artifact_data.get("file_count"),
                                    existing_artifact.created_at,
                                    existing_artifact.file_count,
                                )
                                await session.delete(existing_artifact)
                                await session.flush()
                                logger.warning(
                                    "Duplicate alias found for workspace='%s', alias='%s' — S3 artifact is OLDER (S3: %s, DB: %s). Skipping.\n"
                                    "→ S3 created_at: %s, file_count: %s\n"
                                    "→ DB created_at: %s, file_count: %s",
                                    ws,
                                    alias,
                                    s3_time,
                                    db_time,
                                    artifact_data.get("created_at"),
                                    artifact_data.get("file_count"),
                                    existing_artifact.created_at,
                                    existing_artifact.file_count,
                                )
                                skipped += 1
                                continue

                    artifact = ArtifactModel(**artifact_data)
                    session.add(artifact)
                    inserted += 1
                    logger.info(
                        "Inserted artifact: %s, alias: %s, workspace: %s, file_count: %s, created_at: %s, created_by: %s, last_modified: %s",
                        artifact.id,
                        artifact.alias,
                        artifact.workspace,
                        artifact.file_count,
                        artifact.created_at,
                        artifact.created_by,
                        artifact.last_modified,
                    )
            except Exception as e:
                logger.error("Insert error for artifact %s: %s", artifact_id, e)
                failed += 1

    logger.info(
        "Database recovery completed. Inserted: %d, Overwritten: %d, Skipped: %d, Failed: %d",
        inserted,
        overwritten,
        skipped,
        failed,
    )


async def remove_orphan_artifacts(session_maker):
    async with session_maker() as session:
        result = await session.execute(select(ArtifactModel))
        all_artifacts = result.all()
        id_set = {a.id for a, in all_artifacts}

        orphans = [
            a for a, in all_artifacts if a.parent_id and a.parent_id not in id_set
        ]

        print(f"🧹 Found {len(orphans)} orphan artifacts to delete...")
        for orphan in orphans:
            print(
                f" - Removing: {orphan.id} (alias: {orphan.alias}, missing parent: {orphan.parent_id})"
            )
            await session.delete(orphan)

        await session.commit()
        print("✅ Orphan cleanup complete.")


async def get_artifact(workspace: str, alias: str):
    async with session_maker() as session:
        stmt = select(ArtifactModel).where(
            ArtifactModel.workspace == workspace, ArtifactModel.alias == alias
        )
        result = await session.exec(stmt)
        return result.one_or_none()


async def list_children(parent_id: str):
    async with session_maker() as session:
        stmt = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)
        children = await session.exec(stmt)
        children = children.all()
        print(f"🔗 Found {len(children)} child artifacts:")
        for child in children:
            print(
                f" - {child.alias} ({child.id}), workspace: {child.workspace}, file_count: {child.file_count}, last_modified: {datetime.utcfromtimestamp(child.last_modified).isoformat()}"
            )


async def inspect_collection(workspace: str, alias: str):
    print(f"🔍 Inspecting: {workspace}/{alias}")
    artifact = await get_artifact(workspace, alias)
    if not artifact:
        print("❌ Artifact not found.")
        return

    print(f"🧬 ID: {artifact.id}, Workspace: {artifact.workspace}")
    print(f"📦 Created at: {datetime.utcfromtimestamp(artifact.created_at)}")
    print(f"🛠️  File count: {artifact.file_count}")
    print(f"👤 Created by: {artifact.created_by}")
    print(
        f"📝 Manifest name: {artifact.manifest.get('name') if artifact.manifest else 'N/A'}"
    )
    print(f"📅 Last modified: {datetime.utcfromtimestamp(artifact.last_modified)}")

    await list_children(artifact.id)


# Replace with your PostgreSQL session maker
async def test_raw_list_children(parent_id: str):
    async with pg_session_maker() as session:
        print(f"🔍 Looking for children of artifact: {parent_id}")

        query = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)
        result = await session.execute(query)
        children = result.scalars().all()

        print(f"✅ Found {len(children)} children")
        for a in children[:10]:  # Preview first 10
            created = (
                datetime.utcfromtimestamp(a.created_at).isoformat()
                if a.created_at
                else "?"
            )
            print(
                f" - {a.id}: {a.alias} (created: {created}, file_count: {a.file_count})"
            )


async def debug_list_children_like(
    parent_id: str, stage=None, keyword=None, config_permission_filter=None
):
    async with pg_session_maker() as session:
        print(f"🔍 Testing children of parent: {parent_id}")
        backend = pg_engine.dialect.name
        query = select(ArtifactModel).where(ArtifactModel.parent_id == parent_id)

        # Simulate `stage` filter
        if stage is not None:
            print(f"⚙️ Adding stage filter: {stage}")
            if stage:
                query = query.where(
                    and_(
                        ArtifactModel.staging.isnot(None),
                        text("staging::text != 'null'"),
                    )
                )
            else:
                query = query.where(
                    or_(ArtifactModel.staging.is_(None), text("staging::text = 'null'"))
                )

        # Simulate keyword filter
        if keyword:
            print(f"🔍 Adding keyword filter: '{keyword}'")
            query = query.where(text(f"manifest::text ILIKE '%{keyword}%'"))

        # Simulate config.permissions filter
        if config_permission_filter:
            print(f"🔐 Adding permission filter: {config_permission_filter}")
            for user_id, permission in config_permission_filter.items():
                condition = text(
                    f"config->'permissions'->>'{user_id}' = '{permission}'"
                )
                query = query.where(condition)

        query = query.order_by(ArtifactModel.created_at.asc()).limit(10)
        result = await session.execute(query)
        children = result.scalars().all()

        print(f"✅ Found {len(children)} children")
        for a in children:
            created = datetime.utcfromtimestamp(a.created_at).isoformat()
            print(
                f" - {a.id}: {a.alias} (created: {created}, file_count: {a.file_count})"
            )


async def main():
    await rebuild_database()
    engine = create_async_engine(DATABASE_URI, echo=False)
    sqlite_session_maker = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    await remove_orphan_artifacts(sqlite_session_maker)
    # Run both inspections
    await inspect_collection("bioimage-io", "bioimage.io")
    print("\n" + "-" * 60 + "\n")
    await inspect_collection("shareloc-xyz", "shareloc-collection")
    await test_raw_list_children("019362db-e4b7-7fd1-92b3-dd451eefeaa1")  # bioimage.io

    # ✅ Works: no filters
    await debug_list_children_like("019362db-e4b7-7fd1-92b3-dd451eefeaa1")

    # 🔍 Test stage=False filter (should work if correctly implemented)
    await debug_list_children_like("019362db-e4b7-7fd1-92b3-dd451eefeaa1", stage=False)

    # 🔍 Test stage=True filter (may return fewer or none)
    await debug_list_children_like("019362db-e4b7-7fd1-92b3-dd451eefeaa1", stage=True)

    # 🧪 Test keyword filter — replace with a known string if needed
    await debug_list_children_like(
        "019362db-e4b7-7fd1-92b3-dd451eefeaa1", keyword="crab"
    )

    # 🔐 Simulate permission filter
    await debug_list_children_like(
        "019362db-e4b7-7fd1-92b3-dd451eefeaa1",
        config_permission_filter={"sophisticated-humidity-98268842": "*"},
    )


if __name__ == "__main__":
    asyncio.run(main())
