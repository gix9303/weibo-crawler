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
import os
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
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Image as RLImage,
)
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
class RetweetPost:
    id: str
    created_at: datetime
    text: str
    screen_name: str
    pics: List[str]


@dataclass
class WeiboPost:
    id: str
    created_at: datetime
    text: str
    screen_name: str
    pics: List[str]
    comments: List[Comment]
    retweet: Optional[RetweetPost] = None


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
            SELECT id, text, created_at, pics, retweet_id, screen_name
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

            retweet_obj: Optional[RetweetPost] = None
            retweet_id = (row["retweet_id"] or "").strip()
            if retweet_id:
                retweet_obj = self._load_retweet(conn, retweet_id)

            weibos.append(
                WeiboPost(
                    id=row["id"],
                    created_at=created_at,
                    text=row["text"] or "",
                    screen_name=row["screen_name"] or "",
                    pics=self._split_pics(row["pics"]),
                    comments=comments,
                    retweet=retweet_obj,
                )
            )
        return weibos

    def _load_retweet(self, conn: sqlite3.Connection, retweet_id: str) -> Optional[RetweetPost]:
        """加载被转发的原微博信息（正文 + 图片）"""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, text, created_at, pics, screen_name
            FROM weibo
            WHERE id = ?
            """,
            (retweet_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        created_at = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S")
        return RetweetPost(
            id=row["id"],
            created_at=created_at,
            text=row["text"] or "",
            screen_name=row["screen_name"] or "",
            pics=self._split_pics(row["pics"]),
        )

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

    # --- local image lookup -------------------------------------------------

    def _build_image_index(self, search_root: Path) -> dict[str, List[Path]]:
        """
        在给定目录下构建一份「微博ID -> 图片文件列表」索引。

        约定：
        - weibo.py 下载图片时的文件名形如：
          20241214_1234567890123456.jpg 或 20241214_1234567890123456_1.jpg
        - 即文件名中倒数第二段（或仅有的第二段）为 weibo_id。
        - 图片实际目录结构可能为：
          * 单任务：weibo/<task_id>/<用户目录>/img/原创微博图片/*.jpg 等多级目录
          * 聚合结果：<用户目录>/img/*.jpg
        - 本方法会在 search_root 下递归扫描所有图片文件，并通过文件名解析 weibo_id，
          以保证“微博正文的图片”可以插入到对应微博正文下方。
        """
        exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
        index: dict[str, List[Path]] = {}

        # 递归扫描 search_root 下的所有图片文件；跳过评论图片目录 comments_img，
        # 只索引微博正文图片（文件名中含有微博ID）。
        try:
            for root, dirs, files in os.walk(search_root):
                # 跳过评论图片目录
                if "comments_img" in root.split(os.sep):
                    continue
                for name in files:
                    p = Path(root) / name
                    if not p.is_file():
                        continue
                    if p.suffix.lower() not in exts:
                        continue
                    stem = p.stem  # 形如 20241214_1234567890123456 或 20241214_1234567890123456_1
                    parts = stem.split("_")
                    if len(parts) < 2:
                        continue
                    # 多图：优先使用倒数第二段；单图：最后一段就是 weibo_id
                    candidate_ids = []
                    if len(parts) >= 3:
                        candidate_ids.append(parts[-2])
                    candidate_ids.append(parts[-1])
                    weibo_id = None
                    for cid in candidate_ids:
                        if cid.isdigit():
                            weibo_id = cid
                            break
                    if not weibo_id:
                        continue
                    index.setdefault(weibo_id, []).append(p)
        except Exception:
            # 目录扫描失败时直接返回空索引
            return {}

        # 确保每个微博ID下的图片按文件名排序，避免顺序抖动
        for wid, paths in index.items():
            paths.sort(key=lambda x: x.name)
            index[wid] = paths

        return index

    @staticmethod
    def _split_pics(pics_raw: Optional[str]) -> List[str]:
        """将逗号分隔的图片 URL 转成列表；空值返回空列表"""
        if not pics_raw:
            return []
        if isinstance(pics_raw, str):
            return [p for p in pics_raw.split(",") if p]
        return []

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
            # 解析为带时区的 datetime，然后去掉 tzinfo，统一使用“naive” 时间，
            # 避免在排序时与 datetime.min（naive）比较出现
            # "can't compare offset-naive and offset-aware datetimes" 错误。
            dt = datetime.strptime(raw, "%a %b %d %H:%M:%S %z %Y")
            return dt.replace(tzinfo=None)
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

        # 在 PDF 输出目录附近预构建一份「微博ID -> 图片文件列表」索引，
        # 这样每条微博可以快速找到自己的本地图片并插入到正文下方。
        search_root = output_path.parent
        image_index = self._build_image_index(search_root)
        max_width = A4[0] - 40 * mm  # 页面宽度减去左右边距

        for post in weibos:
            header = f"{post.created_at.strftime('%Y-%m-%d %H:%M:%S')}  微博ID: {post.id}"
            elements.append(Paragraph(escape(header), styles["weibo_header"]))
            elements.append(Spacer(1, 1.5 * mm))

            if post.text:
                elements.append(
                    Paragraph(escape(post.text).replace("\n", "<br/>"), styles["weibo"])
                )
                elements.append(Spacer(1, 2 * mm))

            self._append_images_for_post(post.id, image_index, elements, max_width)

            # 若为转发微博，追加被转发的原微博正文与图片
            if post.retweet:
                rt = post.retweet
                rt_header = f"转发自 {rt.screen_name or '原微博'}（ID: {rt.id}，时间: {rt.created_at.strftime('%Y-%m-%d %H:%M:%S')}）"
                elements.append(Paragraph(escape(rt_header), styles["retweet_header"]))
                elements.append(Spacer(1, 1 * mm))
                if rt.text:
                    elements.append(
                        Paragraph(escape(rt.text).replace("\n", "<br/>"), styles["retweet_body"])
                    )
                    elements.append(Spacer(1, 1.5 * mm))
                self._append_images_for_post(rt.id, image_index, elements, max_width)

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

    def _append_images_for_post(
        self, post_id: str, image_index: dict[str, List[Path]], elements: List[object], max_width: float
    ) -> None:
        """将某条微博（或转发微博）的正文图片追加到 PDF 元素列表中"""
        img_paths = image_index.get(post_id) or []
        for img_path in img_paths:
            try:
                img = RLImage(str(img_path))
                # 超宽图片按页面宽度等比缩放
                if img.drawWidth > max_width:
                    factor = float(max_width) / float(img.drawWidth or max_width)
                    img.drawWidth = max_width
                    img.drawHeight = img.drawHeight * factor
                elements.append(img)
                elements.append(Spacer(1, 2 * mm))
            except Exception:
                # 单张图片出问题时忽略，继续生成其它内容
                continue

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
        styles.add(
            ParagraphStyle(
                name="RetweetHeader",
                parent=styles["Normal"],
                fontName=base_font,
                fontSize=10,
                leading=13,
                textColor="#555555",
            )
        )
        styles.add(
            ParagraphStyle(
                name="RetweetBody",
                parent=styles["Normal"],
                fontName=base_font,
                fontSize=10,
                leading=13,
                textColor="#444444",
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
            "retweet_header": styles["RetweetHeader"],
            "retweet_body": styles["RetweetBody"],
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
