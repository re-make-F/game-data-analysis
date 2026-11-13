#!/usr/bin/env python
"""把原始 CSV 导入 MySQL，并做一次行数/销量校验。

从 notebook 里分离出来的原因：
  · 入库是**一次性、可重复执行**的准备工作，不属于分析过程；
  · notebook 里每次「Run All」都重跑一遍 DROP + 全量写入，纯属浪费；
  · 入库出错（连不上、密码错、编码乱）时，应该在命令行里直接看到清晰报错，
    而不是淹没在 notebook 的一堆输出里。

用法：
    python scripts/load_to_mysql.py                     # 用默认配置
    python scripts/load_to_mysql.py --dry-run           # 只检查，不写库
    python scripts/load_to_mysql.py --verify-only       # 只校验已入库的数据
    python scripts/load_to_mysql.py --if-exists append  # 追加而不是覆盖

退出码：0 成功，1 配置/连接错误，2 校验不通过。
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

# 项目根目录 = scripts/ 的上一级
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = ROOT / "data" / "video_games_sales.csv"
DEFAULT_DATABASE = "data_analysis"
DEFAULT_TABLE = "video_games_sales"

#: 原始 CSV 的预期值，用来做入库后的完整性校验。
#: 注意这里校验的是**原始数据**（16719 行 / 8920.30），不是清洗后的
#: 16715 行 / 8917.88 —— 清洗发生在 notebook 里，入库只负责搬运原始表。
EXPECTED_ROWS = 16719
EXPECTED_SALES = 8920.30


def find_env(explicit: str | None) -> Path | None:
    """定位 .env：优先命令行指定，其次项目根目录，最后兼容旧的 notebook/.env。"""
    if explicit:
        p = Path(explicit)
        if not p.is_file():
            raise FileNotFoundError(f"指定的 .env 不存在：{p}")
        return p
    for cand in (ROOT / ".env", ROOT / "notebook" / ".env"):
        if cand.is_file():
            return cand
    return None


def read_db_config(env_path: Path | None) -> dict[str, str]:
    """从 .env 读数据库配置，缺项时给出可操作的报错。"""
    if env_path:
        load_dotenv(env_path)
        print(f"[env] 已加载 {env_path}")
    else:
        print("[env] 未找到 .env，尝试使用当前进程的环境变量")

    cfg = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }
    missing = [k.upper() for k, v in cfg.items() if not v]
    if missing:
        raise SystemExit(
            "缺少数据库配置：%s\n"
            "请复制 .env.example 为 .env 并填写，例如：\n"
            "  DB_USER=root\n  DB_PASSWORD=your_password\n"
            "  DB_HOST=127.0.0.1\n  DB_PORT=3306\n  DB_NAME=your_db_name"
            % ", ".join(missing)
        )

    # localhost 在 Windows 上会先尝试 IPv6 ::1，MySQL 只监听 IPv4 时要等超时才回落。
    # 实测首连 2075ms vs 42ms，这里主动提示一次，但不擅自改用户配置。
    if cfg["host"] == "localhost":
        print("[提示] DB_HOST=localhost 在 Windows 上可能让首次连接多等约 2 秒；"
              "改成 127.0.0.1 可以避免。")

    return {k: str(v) for k, v in cfg.items()}


def build_url(cfg: dict[str, str], database: str | None = None) -> URL:
    """用 URL.create 拼连接串。

    不要用 f"mysql+pymysql://{user}:{password}@..." ——
    密码里只要出现 @ : / ? # 之类的字符，URL 就会被解析错。
    URL.create 会自动做百分号编码。
    """
    return URL.create(
        drivername="mysql+pymysql",
        username=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=int(cfg["port"]),
        database=database,
    )


def connect(cfg: dict[str, str], database: str | None) -> Engine:
    return create_engine(build_url(cfg, database), pool_pre_ping=True)


def ensure_database(engine: Engine, name: str) -> None:
    """建库（幂等）。数据库名不能参数化，所以这里只允许安全标识符。"""
    if not name.replace("_", "").isalnum():
        raise SystemExit(f"数据库名只允许字母数字下划线：{name!r}")
    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE DATABASE IF NOT EXISTS `{name}` "
            "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        ))
        conn.commit()
    print(f"[db] 数据库就绪：{name}")


def load_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.is_file():
        raise SystemExit(f"CSV 不存在：{csv_path}")
    t0 = time.perf_counter()
    df = pd.read_csv(csv_path)
    print(f"[csv] 读取 {csv_path.name}：{len(df):,} 行 × {df.shape[1]} 列，"
          f"耗时 {time.perf_counter() - t0:.2f}s")
    return df


def load(df: pd.DataFrame, engine: Engine, table: str, if_exists: str,
         chunksize: int) -> float:
    t0 = time.perf_counter()
    df.to_sql(
        table,
        con=engine,
        if_exists=if_exists,   # replace / append / fail
        index=False,           # 不把 pandas 行索引写成额外一列
        chunksize=chunksize,   # 分块写入，避免一次构造超大 INSERT
    )
    elapsed = time.perf_counter() - t0
    print(f"[load] 写入 {table}：{len(df):,} 行，模式={if_exists}，"
          f"耗时 {elapsed:.2f}s")
    return elapsed


def verify(engine: Engine, database: str, table: str) -> bool:
    """校验行数与销量总额，确认写入没有静默丢数据。"""
    q = f"SELECT COUNT(*) AS n, SUM(Global_Sales) AS s FROM `{database}`.`{table}`"
    with engine.connect() as conn:
        row = conn.execute(text(q)).one()
    n, s = int(row[0]), float(row[1] or 0)

    ok_rows = n == EXPECTED_ROWS
    ok_sales = abs(s - EXPECTED_SALES) < 0.01
    print(f"[verify] 行数 {n:,}（期望 {EXPECTED_ROWS:,}）{'✓' if ok_rows else '✗'}")
    print(f"[verify] 销量合计 {s:.2f}（期望 {EXPECTED_SALES:.2f}）"
          f"{'✓' if ok_sales else '✗'}")
    return ok_rows and ok_sales


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="把游戏销量 CSV 导入 MySQL")
    ap.add_argument("--csv", default=str(DEFAULT_CSV), help="CSV 路径")
    ap.add_argument("--database", default=DEFAULT_DATABASE, help="目标数据库名")
    ap.add_argument("--table", default=DEFAULT_TABLE, help="目标表名")
    ap.add_argument("--if-exists", default="replace",
                    choices=("replace", "append", "fail"),
                    help="表已存在时的行为（默认 replace）")
    ap.add_argument("--chunksize", type=int, default=5000, help="分块写入大小")
    ap.add_argument("--env", default=None, help=".env 路径")
    ap.add_argument("--dry-run", action="store_true", help="只检查配置与数据，不写库")
    ap.add_argument("--verify-only", action="store_true", help="只校验已入库的数据")
    args = ap.parse_args(argv)

    try:
        env_path = find_env(args.env)
        cfg = read_db_config(env_path)

        if args.verify_only:
            eng = connect(cfg, args.database)
            return 0 if verify(eng, args.database, args.table) else 2

        df = load_csv(Path(args.csv))

        if args.dry_run:
            print("[dry-run] 配置与数据检查通过，未写入数据库")
            print(f"[dry-run] 将写入 {args.database}.{args.table}，"
                  f"模式={args.if_exists}，分块={args.chunksize}")
            return 0

        # 先连 server（不指定库），因为目标库可能还不存在
        server = connect(cfg, None)
        ensure_database(server, args.database)
        server.dispose()

        target = connect(cfg, args.database)
        load(df, target, args.table, args.if_exists, args.chunksize)
        ok = verify(target, args.database, args.table)
        target.dispose()
        return 0 if ok else 2

    except SystemExit:
        raise
    except Exception as exc:  # 连接失败、认证失败、驱动缺失等
        print(f"[错误] {type(exc).__name__}: {exc}", file=sys.stderr)
        hint = {
            "OperationalError": "检查 MySQL 是否启动、主机端口是否正确、账号密码是否有效",
            "ModuleNotFoundError": "缺少依赖，先运行 pip install -r requirements.txt",
        }
        for key, msg in hint.items():
            if key in type(exc).__name__:
                print(f"[提示] {msg}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
