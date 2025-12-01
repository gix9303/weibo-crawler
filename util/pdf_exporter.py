#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Lightweight PDF exporter for weibo data stored in SQLite.

Features:
- Reads user, weibo and comment data from weibodata.db
- Weibo groups sorted by publish time (newest -> oldest)
- Each group: one weibo paragraph followed by its comments
- Comments sorted by like_count (highest -> lowest), limited by config
- First page contains basic user profile information
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer
from xml.sax.saxutils import escape


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = BASE_DIR / "weibo" / "weibodata.db"


@dataclass
class UserProfile:
    id: str
    nick_name: str
    gender: str
    follower_count: int
    follow_count: int
    birthday: str
    location: str
    edu: str
    company: str
    reg_date: str
    main_page_url: str
    bio: str


@dataclass
class Comment:
    id: str
    created_at_raw: str
    created_at: Optional[datetime]
    user_screen_name: str
    text: str
    like_count: int


@dataclass
class WeiboPost:
    id: str
    created_at: datetime
    text: str
    comments: List[Comment]


class WeiboPdfExporter:
    """Export one user's timeline with comments into a PDF."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        comment_limit: Optional[int] = None,
    ) -> None:
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        if not self.db_path.exists():
            raise FileNotFoundError(f"SQLite database not found: {self.db_path}")
        # 控制每条微博写入 PDF 的评论数量，优先使用传入参数，否则读取 config.json 中的 comment_by_like_count
        self.comment_limit = self._load_comment_limit(comment_limit)
        # 是否按年拆分 PDF，从 config.json 中读取 pdf.split_by_year，默认 False
        self.split_by_year = self._load_split_by_year()

    def _load_comment_limit(self, override: Optional[int]) -> int:
        """Resolve comment limit from explicit arg or config.json, fallback to 10."""
        if isinstance(override, int) and override > 0:
            return override

        config_path = BASE_DIR / "config.json"
        try:
            with config_path.open(encoding="utf-8") as f:
                cfg = json.load(f)
            value = cfg.get("comment_by_like_count")
            if isinstance(value, int) and value > 0:
                return value
        except Exception:
            # On any error (missing file, bad json, missing key), fall back to default
            pass

        return 10

    def _load_split_by_year(self) -> bool:
        """从 config.json 读取 pdf.split_by_year 开关，默认 False。"""
        config_path = BASE_DIR / "config.json"
        try:
            with config_path.open(encoding="utf-8") as f:
                cfg = json.load(f)
            pdf_cfg = cfg.get("pdf") or {}
            if isinstance(pdf_cfg, dict):
                val = pdf_cfg.get("split_by_year")
                return bool(val)
        except Exception:
            pass
        return False

    # --- public API ---------------------------------------------------------

    def export_user_timeline(
        self,
        user_id: str,
        output_path: Path | str | None,
        font_path: Optional[Path | str] = None,
    ) -> Path:
        """
        Export timeline and comments of one user into one or more PDF files.

        - 如果该用户的微博只跨单一年份：生成单个 PDF（保持原有行为）
        - 如果跨越多个年份：按年份拆分为多个 PDF（每个 PDF 只包含该年份的微博）

        :param user_id: weibo user id
        :param output_path: target pdf path; if None, auto-generate under weibo/ directory
        :param font_path: optional TTF font path for CJK text
        :return: Path to the最后一个生成的 pdf（跨年时返回最后一年的那个）
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            user = self._load_user(conn, user_id)
            if not user:
                raise ValueError(f"User {user_id} not found in SQLite.")

            weibos = self._load_weibos_with_comments(conn, user_id)

        # 没有微博时，仍按原逻辑生成一个空 PDF
        if not weibos:
            if output_path is None:
                output_path = self._build_default_output_path(user, weibos)
            output_path = Path(output_path)
            self._build_pdf(user, weibos, output_path, font_path)
            return output_path

        # 若未开启“按年拆分”，或实际上只有单一年份，则保持原有单 PDF 行为
        weibos_by_year: dict[int, list[WeiboPost]] = {}
        for w in weibos:
            year = w.created_at.year
            weibos_by_year.setdefault(year, []).append(w)

        years = sorted(weibos_by_year.keys())

        if (not self.split_by_year) or len(years) == 1:
            if output_path is None:
                output_path = self._build_default_output_path(user, weibos)
            output_path = Path(output_path)
            self._build_pdf(user, weibos, output_path, font_path)
            return output_path

        # 多个年份 -> 按年拆分 PDF
        last_path: Optional[Path] = None

        if output_path is not None:
            # 调用方指定了输出文件，例如 foo.pdf
            # 拆分后输出为 foo_2023.pdf、foo_2024.pdf ...
            base = Path(output_path)
            stem = base.stem
            suffix = base.suffix or ".pdf"

            for year in years:
                posts = weibos_by_year.get(year) or []
                if not posts:
                    continue
                year_path = base.with_name(f"{stem}_{year}{suffix}")
                self._build_pdf(user, posts, year_path, font_path)
                last_path = year_path
        else:
            # 未指定输出路径：沿用默认命名规则，但针对每个年份单独计算起止日期
            weibo_dir = BASE_DIR / "weibo"
            weibo_dir.mkdir(exist_ok=True)

            nick = user.nick_name or user.id
            safe_nick = re.sub(r'[\\/:*?"<>|]', "_", str(nick))

            for year in years:
                posts = weibos_by_year.get(year) or []
                if not posts:
                    continue
                dates = [
                    p.created_at
                    for p in posts
                    if isinstance(p.created_at, datetime)
                ]
                if dates:
                    start_str = min(dates).strftime("%Y-%m-%d")
                    end_str = max(dates).strftime("%Y-%m-%d")
                else:
                    start_str = end_str = "unknown"
                filename = f"{safe_nick}_{start_str}_{end_str}.pdf"
                year_path = weibo_dir / filename
                self._build_pdf(user, posts, year_path, font_path)
                last_path = year_path

        if last_path is None:
            # 理论上不会走到这里，防御性兜底：退回到单文件模式
            if output_path is None:
                output_path = self._build_default_output_path(user, weibos)
            last_path = Path(output_path)
            self._build_pdf(user, weibos, last_path, font_path)

        return last_path

    def _build_default_output_path(
        self, user: UserProfile, weibos: List[WeiboPost]
    ) -> Path:
        """
        Build default PDF path: weibo/<nick_name>_<start_date>_<end_date>.pdf
        Dates are derived from the actual weibo data range.
        """
        weibo_dir = BASE_DIR / "weibo"
        weibo_dir.mkdir(exist_ok=True)

        nick = user.nick_name or user.id
        safe_nick = re.sub(r'[\\/:*?"<>|]', "_", str(nick))

        if weibos:
            dates = [w.created_at for w in weibos if isinstance(w.created_at, datetime)]
            if dates:
                start_str = min(dates).strftime("%Y-%m-%d")
                end_str = max(dates).strftime("%Y-%m-%d")
            else:
                start_str = end_str = "unknown"
        else:
            start_str = end_str = "unknown"

        filename = f"{safe_nick}_{start_str}_{end_str}.pdf"
        return weibo_dir / filename

    # --- data access --------------------------------------------------------

    def _load_user(self, conn: sqlite3.Connection, user_id: str) -> Optional[UserProfile]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, nick_name, gender, follower_count, follow_count,
                   birthday, location, edu, company, reg_date,
                   main_page_url, bio
            FROM user
            WHERE id = ?
            """,
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return UserProfile(
            id=row["id"],
            nick_name=row["nick_name"],
            gender=row["gender"],
            follower_count=row["follower_count"] or 0,
            follow_count=row["follow_count"] or 0,
            birthday=row["birthday"] or "",
            location=row["location"] or "",
            edu=row["edu"] or "",
            company=row["company"] or "",
            reg_date=row["reg_date"] or "",
            main_page_url=row["main_page_url"] or "",
            bio=row["bio"] or "",
        )

    def _load_weibos_with_comments(
        self, conn: sqlite3.Connection, user_id: str
    ) -> List[WeiboPost]:
        cur = conn.cursor()
        # created_at is stored as "YYYY-MM-DD HH:MM:SS"
        cur.execute(
            """
            SELECT id, text, created_at
            FROM weibo
            WHERE user_id = ?
            ORDER BY datetime(created_at) DESC
            """,
            (user_id,),
        )
        weibo_rows = cur.fetchall()

        weibos: List[WeiboPost] = []
        for row in weibo_rows:
            created_at = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S")
            comments = self._load_comments_for_weibo(conn, row["id"])
            weibos.append(
                WeiboPost(
                    id=row["id"],
                    created_at=created_at,
                    text=row["text"] or "",
                    comments=comments,
                )
            )
        return weibos

    def _load_comments_for_weibo(
        self, conn: sqlite3.Connection, weibo_id: str
    ) -> List[Comment]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, created_at, user_screen_name, text, like_count
            FROM comments
            WHERE weibo_id = ?
            """,
            (weibo_id,),
        )
        rows = cur.fetchall()

        comments: List[Comment] = []
        for row in rows:
            raw = row["created_at"] or ""
            dt = self._parse_comment_time(raw)
            comments.append(
                Comment(
                    id=row["id"],
                    created_at_raw=raw,
                    created_at=dt,
                    user_screen_name=row["user_screen_name"] or "",
                    text=row["text"] or "",
                    like_count=row["like_count"] or 0,
                )
            )

        # sort comments by like_count descending, then by time/id for stable ordering
        comments.sort(
            key=lambda c: (-c.like_count, c.created_at or datetime.min, c.id)
        )

        # limit number of comments written into PDF for each weibo
        limit = self.comment_limit
        if isinstance(limit, int) and limit > 0:
            comments = comments[:limit]

        return comments

    @staticmethod
    def _parse_comment_time(raw: str) -> Optional[datetime]:
        """
        Parse comment created_at string from SQLite.
        Sample: 'Thu Nov 20 11:39:50 +0800 2025'
        """
        raw = (raw or "").strip()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%a %b %d %H:%M:%S %z %Y")
        except Exception:
            return None

    # --- pdf rendering ------------------------------------------------------

    def _build_pdf(
        self,
        user: UserProfile,
        weibos: List[WeiboPost],
        output_path: Path,
        font_path: Optional[Path | str] = None,
    ) -> None:
        doc = SimpleDocTemplate(
            str(output_path),
            pagesize=A4,
            rightMargin=20 * mm,
            leftMargin=20 * mm,
            topMargin=20 * mm,
            bottomMargin=20 * mm,
        )

        styles = self._init_styles(font_path)
        elements: List[object] = []

        # first page: user profile
        elements.append(Paragraph("微博用户信息", styles["title"]))
        elements.append(Spacer(1, 6 * mm))

        gender = {"m": "男", "f": "女"}.get(user.gender, user.gender or "")
        profile_lines = [
            f"用户ID: {user.id}",
            f"昵称: {user.nick_name}",
            f"性别: {gender}",
            f"粉丝数: {user.follower_count}",
            f"关注数: {user.follow_count}",
            f"生日: {user.birthday}",
            f"所在地: {user.location}",
            f"教育: {user.edu}",
            f"公司: {user.company}",
            f"注册时间: {user.reg_date}",
            f"主页: {user.main_page_url}",
            f"简介: {user.bio}",
        ]
        for line in profile_lines:
            safe = escape(line)
            elements.append(Paragraph(safe, styles["normal"]))
            elements.append(Spacer(1, 2 * mm))

        # timeline starts from page 2
        elements.append(PageBreak())

        for post in weibos:
            header = f"{post.created_at.strftime('%Y-%m-%d %H:%M:%S')}  微博ID: {post.id}"
            elements.append(Paragraph(escape(header), styles["weibo_header"]))
            elements.append(Spacer(1, 1.5 * mm))

            if post.text:
                elements.append(
                    Paragraph(escape(post.text).replace("\n", "<br/>"), styles["weibo"])
                )
                elements.append(Spacer(1, 2 * mm))

            if post.comments:
                elements.append(Paragraph("评论：", styles["comment_header"]))
                elements.append(Spacer(1, 1.2 * mm))
                for c in post.comments:
                    t_str = (
                        c.created_at.strftime("%Y-%m-%d %H:%M:%S")
                        if c.created_at
                        else c.created_at_raw
                    )
                    line = f"[{t_str}] {c.user_screen_name}: {c.text}"
                    elements.append(
                        Paragraph(escape(line).replace("\n", " "), styles["comment"])
                    )
                    elements.append(Spacer(1, 0.8 * mm))

            elements.append(Spacer(1, 4 * mm))

        doc.build(elements)

    def _init_styles(self, font_path: Optional[Path | str]) -> dict:
        styles = getSampleStyleSheet()

        # 默认使用 STSong-Light（Adobe-GB1 中文字体），保证中文/英文都能正常显示。
        # 如果调用方显式传入 font_path，则优先使用该字体。
        base_font = "STSong-Light"
        try:
            pdfmetrics.getFont(base_font)
        except KeyError:
            pdfmetrics.registerFont(UnicodeCIDFont(base_font))

        if font_path:
            custom_path = Path(font_path)
            if custom_path.exists():
                pdfmetrics.registerFont(TTFont("CustomFont", str(custom_path)))
                base_font = "CustomFont"

        styles.add(
            ParagraphStyle(
                name="WeiboTitle",
                parent=styles["Heading1"],
                fontName=base_font,
                fontSize=18,
                leading=22,
                spaceAfter=6 * mm,
            )
        )
        styles.add(
            ParagraphStyle(
                name="WeiboHeader",
                parent=styles["Heading3"],
                fontName=base_font,
                fontSize=12,
                leading=15,
                spaceAfter=1 * mm,
            )
        )
        styles.add(
            ParagraphStyle(
                name="WeiboBody",
                parent=styles["Normal"],
                fontName=base_font,
                fontSize=11,
                leading=15,
            )
        )
        styles.add(
            ParagraphStyle(
                name="CommentHeader",
                parent=styles["Normal"],
                fontName=base_font,
                fontSize=11,
                leading=14,
                spaceBefore=1 * mm,
                spaceAfter=0.5 * mm,
            )
        )
        styles.add(
            ParagraphStyle(
                name="CommentBody",
                parent=styles["Normal"],
                fontName=base_font,
                fontSize=9,
                leading=12,
            )
        )

        return {
            "title": styles["WeiboTitle"],
            "weibo_header": styles["WeiboHeader"],
            "weibo": styles["WeiboBody"],
            "comment_header": styles["CommentHeader"],
            "comment": styles["CommentBody"],
            "normal": ParagraphStyle(
                name="NormalCN",
                parent=styles["Normal"],
                fontName=base_font,
                fontSize=11,
                leading=14,
            ),
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Export one user's weibo timeline and comments to PDF."
    )
    parser.add_argument("user_id", help="weibo user id stored in SQLite")
    parser.add_argument(
        "output",
        nargs="?",
        help="output pdf path (optional, default: weibo/<nick>_<start>_<end>.pdf)",
        default=None,
    )
    parser.add_argument(
        "--db",
        help=f"SQLite database path (default: {DEFAULT_DB_PATH})",
        default=str(DEFAULT_DB_PATH),
    )
    parser.add_argument(
        "--font",
        help="Optional TTF font path for CJK text (e.g. a Chinese system font)",
        default=None,
    )
    args = parser.parse_args()

    exporter = WeiboPdfExporter(db_path=Path(args.db))
    pdf_path = exporter.export_user_timeline(
        user_id=args.user_id, output_path=args.output, font_path=args.font
    )
    print(f"PDF generated at: {pdf_path}")
