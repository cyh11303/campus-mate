"""
캠퍼스 메이트 백엔드 서버
- 해시 테이블 (SIZE=31, Separate Chaining)
- Weighted Jaccard 매칭
- SQLite 데이터베이스 연동으로 데이터 영속성 추가
- In-memory TTL 캐시
- 밥약 신청 API
- 밥약 인증 갤러리 API (파일 업로드)
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List, Dict
import time
import threading
import os
import uuid
import sqlite3
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOADS_DIR = os.environ.get("UPLOADS_DIR", os.path.join(BASE_DIR, "uploads"))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(BASE_DIR, "campus_mate.db"))
os.makedirs(UPLOADS_DIR, exist_ok=True)

app = FastAPI(title="캠퍼스 메이트 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ══════════════════════════════════════════════════════════════════════════════
# SQLite DB 설정
# ══════════════════════════════════════════════════════════════════════════════

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """프로그램 실행 시 필요한 테이블을 없으면 자동 생성한다."""
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                sid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                dept TEXT DEFAULT '',
                gender TEXT DEFAULT '',
                smoke TEXT DEFAULT '',
                contact_type TEXT DEFAULT '',
                contact_detail TEXT DEFAULT '',
                traits TEXT NOT NULL DEFAULT '[]',
                pf_gender TEXT DEFAULT 'any',
                pf_smoke TEXT DEFAULT 'any',
                wake_time INTEGER,
                sleep_time INTEGER,
                slot INTEGER NOT NULL,
                chain_index INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meals (
                studentId TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                menu TEXT NOT NULL,
                contact TEXT DEFAULT '',              -- 기존 버전 호환용
                contact_type TEXT DEFAULT '',         -- 카카오톡 ID / 오픈채팅 / 인스타 ID / 이메일 / 기타
                contact_detail TEXT DEFAULT '',       -- 실제 ID, 링크, 이메일 등
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meal_requests (
                id TEXT PRIMARY KEY,
                target_student_id TEXT NOT NULL,
                requester_student_id TEXT NOT NULL,
                requester_name TEXT NOT NULL,
                requester_contact TEXT NOT NULL DEFAULT '',
                message TEXT DEFAULT '',
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(target_student_id) REFERENCES meals(studentId)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gallery_posts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                menu TEXT NOT NULL,
                tags TEXT NOT NULL DEFAULT '[]',
                image_url TEXT NOT NULL,
                yummy INTEGER DEFAULT 0,
                where_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trait_weights (
                trait_key TEXT PRIMARY KEY,
                weight REAL NOT NULL
            )
        """)
        # 기존 campus_mate.db를 그대로 쓰는 경우를 위한 간단한 마이그레이션
        cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        if "smoke" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN smoke TEXT DEFAULT ''")
        if "contact_type" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN contact_type TEXT DEFAULT ''")
        if "contact_detail" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN contact_detail TEXT DEFAULT ''")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS roommate_requests (
                id TEXT PRIMARY KEY,
                target_sid TEXT NOT NULL,
                requester_sid TEXT NOT NULL,
                requester_name TEXT NOT NULL,
                message TEXT DEFAULT '',
                status TEXT DEFAULT 'pending',
                cancel_requested_by TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(target_sid) REFERENCES users(sid)
            )
        """)

        meal_cols = [r[1] for r in conn.execute("PRAGMA table_info(meals)").fetchall()]
        if "contact" not in meal_cols:
            conn.execute("ALTER TABLE meals ADD COLUMN contact TEXT DEFAULT ''")
        if "contact_type" not in meal_cols:
            conn.execute("ALTER TABLE meals ADD COLUMN contact_type TEXT DEFAULT ''")
        if "contact_detail" not in meal_cols:
            conn.execute("ALTER TABLE meals ADD COLUMN contact_detail TEXT DEFAULT ''")
        # v5 이전 DB에 contact 하나로 저장된 값이 있으면 새 구조에도 옮겨 둔다.
        conn.execute("""
            UPDATE meals
            SET contact_type = CASE WHEN contact_type = '' AND contact != '' THEN '기타' ELSE contact_type END,
                contact_detail = CASE WHEN contact_detail = '' AND contact != '' THEN contact ELSE contact_detail END
            WHERE contact != ''
        """)

        req_cols = [r[1] for r in conn.execute("PRAGMA table_info(meal_requests)").fetchall()]
        if req_cols and "requester_contact" not in req_cols:
            conn.execute("ALTER TABLE meal_requests ADD COLUMN requester_contact TEXT DEFAULT ''")

        roommate_req_cols = [r[1] for r in conn.execute("PRAGMA table_info(roommate_requests)").fetchall()]
        if roommate_req_cols and "cancel_requested_by" not in roommate_req_cols:
            conn.execute("ALTER TABLE roommate_requests ADD COLUMN cancel_requested_by TEXT DEFAULT ''")
        conn.commit()

def row_to_user(row: sqlite3.Row) -> dict:
    return {
        "sid": row["sid"],
        "name": row["name"],
        "dept": row["dept"] or "",
        "gender": row["gender"] or "",
        "smoke": row["smoke"] or "",
        "contact_type": row["contact_type"] if "contact_type" in row.keys() else "",
        "contact_detail": row["contact_detail"] if "contact_detail" in row.keys() else "",
        "traits": [t for t in json.loads(row["traits"] or "[]") if t in weights] if "weights" in globals() else json.loads(row["traits"] or "[]"),
        "pf_gender": row["pf_gender"] or "any",
        "pf_smoke": row["pf_smoke"] or "any",
        "wake_time": row["wake_time"],
        "sleep_time": row["sleep_time"],
        "slot": row["slot"],
        "chain_index": row["chain_index"],
    }

def row_to_meal(row: sqlite3.Row) -> dict:
    contact_type = row["contact_type"] if "contact_type" in row.keys() else ""
    contact_detail = row["contact_detail"] if "contact_detail" in row.keys() else ""
    planned_count = row["planned_count"] if "planned_count" in row.keys() else 0
    return {
        "studentId": row["studentId"],
        "name": row["name"],
        "menu": row["menu"],
        "contactType": contact_type or "기타",
        # 신청 현황 목록에 연락처를 노출하지는 않지만, 요청 수락 후 조회를 위해 API에는 보존한다.
        "contactDetail": contact_detail,
        # 해당 등록자에게 온 요청 중 accepted 상태인 요청만 카운트한다.
        "plannedCount": planned_count or 0,
        "created_at": row["created_at"],
    }

def row_to_post(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "menu": row["menu"],
        "tags": json.loads(row["tags"] or "[]"),
        "image_url": row["image_url"],
        "created_at": row["created_at"],
        "reactions": {
            "yummy": row["yummy"],
            "where": row["where_count"],
        },
    }

# ══════════════════════════════════════════════════════════════════════════════
# 해시 테이블 (Separate Chaining, SIZE=31)
# ══════════════════════════════════════════════════════════════════════════════

TABLE_SIZE = 31
HASH_MULTIPLIER = 37

def hash_func(sid: str) -> int:
    """31 슬롯용 누적 해시: h = (h × 37 + charCode[i]) % TABLE_SIZE"""
    h = 0
    for ch in sid:
        h = (h * HASH_MULTIPLIER + ord(ch)) % TABLE_SIZE
    return h

def get_partition(slot: int) -> str:
    return "Primary" if slot < 10 else "Secondary"

hash_table: List[List[dict]] = [[] for _ in range(TABLE_SIZE)]

def rebuild_hash_table_from_db():
    """DB의 users 테이블을 읽어 서버 메모리의 해시 테이블을 재구성한다."""
    global hash_table
    hash_table = [[] for _ in range(TABLE_SIZE)]
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM users ORDER BY created_at ASC, sid ASC").fetchall()

    for row in rows:
        user = row_to_user(row)
        slot = hash_func(user["sid"])
        user["slot"] = slot
        user["chain_index"] = len(hash_table[slot])
        hash_table[slot].append(user)

    sync_user_positions_to_db()

def sync_user_positions_to_db():
    """해시 슬롯/체인 인덱스가 변경되었을 때 DB에도 반영한다."""
    with get_conn() as conn:
        for slot, chain in enumerate(hash_table):
            for idx, user in enumerate(chain):
                user["slot"] = slot
                user["chain_index"] = idx
                conn.execute(
                    "UPDATE users SET slot = ?, chain_index = ? WHERE sid = ?",
                    (slot, idx, user["sid"]),
                )
        conn.commit()

def ht_insert(user: dict) -> int:
    slot = hash_func(user["sid"])
    for u in hash_table[slot]:
        if u["sid"] == user["sid"]:
            raise ValueError("이미 등록된 학번입니다.")
    user["slot"] = slot
    user["chain_index"] = len(hash_table[slot])
    hash_table[slot].append(user)
    return slot

def ht_find(sid: str):
    slot = hash_func(sid)
    for u in hash_table[slot]:
        if u["sid"] == sid:
            return u, slot
    return None, slot

def ht_all_users() -> List[dict]:
    return [u for chain in hash_table for u in chain]

# ══════════════════════════════════════════════════════════════════════════════
# 성향 & 가중치
# ══════════════════════════════════════════════════════════════════════════════

TRAITS_META = [
    {"key": "clean",      "label": "청결 중시",    "weight": 1.2},
    {"key": "study",      "label": "방에서 공부",  "weight": 1.2},
    {"key": "quiet",      "label": "조용한 환경",  "weight": 1.0},
    {"key": "social",     "label": "사교적",       "weight": 1.0},
    {"key": "eat_out",    "label": "외식 선호",    "weight": 1.0},
    {"key": "drink",      "label": "애주가",       "weight": 1.0},
    {"key": "game",       "label": "게임/취미",    "weight": 1.0},
    {"key": "share",      "label": "물건 공유 OK", "weight": 1.0},
    {"key": "guest",      "label": "손님 초대 OK", "weight": 1.0},
    {"key": "fashion",    "label": "패션",         "weight": 1.0},
    {"key": "boardgame",  "label": "보드게임",     "weight": 1.0},
    {"key": "gym",        "label": "헬스",         "weight": 1.0},
    {"key": "soccer",     "label": "축구",         "weight": 1.0},
    {"key": "beauty",     "label": "뷰티",         "weight": 1.0},
    {"key": "foodie",     "label": "맛집탐방",     "weight": 1.0},
]

weights: Dict[str, float] = {t["key"]: t["weight"] for t in TRAITS_META}

def load_weights_from_db():
    with get_conn() as conn:
        for t in TRAITS_META:
            conn.execute(
                "INSERT OR IGNORE INTO trait_weights (trait_key, weight) VALUES (?, ?)",
                (t["key"], t["weight"]),
            )
        rows = conn.execute("SELECT trait_key, weight FROM trait_weights").fetchall()
        conn.commit()

    with get_conn() as conn:
        for row in rows:
            if row["trait_key"] in weights:
                # 가중치는 프론트/백엔드 모두 1.0~2.0 범위로 통일
                new_weight = round(max(1.0, min(2.0, float(row["weight"]))), 1)
                weights[row["trait_key"]] = new_weight
                conn.execute(
                    "UPDATE trait_weights SET weight = ? WHERE trait_key = ?",
                    (new_weight, row["trait_key"]),
                )
        conn.commit()

# ══════════════════════════════════════════════════════════════════════════════
# In-memory 캐시 (TTL 60초)
# ══════════════════════════════════════════════════════════════════════════════

_cache: Dict[str, dict] = {}
_cache_lock = threading.Lock()

def cache_get(key: str):
    with _cache_lock:
        item = _cache.get(key)
        if item and item["expires_at"] > time.time():
            return item["data"]
        if item:
            del _cache[key]
        return None

def cache_set(key: str, data, ttl: int = 60):
    with _cache_lock:
        _cache[key] = {"data": data, "expires_at": time.time() + ttl}

def cache_flush():
    with _cache_lock:
        _cache.clear()

# ══════════════════════════════════════════════════════════════════════════════
# 갤러리 설정
# ══════════════════════════════════════════════════════════════════════════════

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
MAX_FILE_SIZE = 10 * 1024 * 1024

# ══════════════════════════════════════════════════════════════════════════════
# 매칭 알고리즘
# ══════════════════════════════════════════════════════════════════════════════

def weighted_jaccard(traits_a: List[str], traits_b: List[str]) -> float:
    """score(A,B) = Σ w(t)·[t∈A∩B] / Σ w(t)·[t∈A∪B]"""
    set_a, set_b = set(traits_a), set(traits_b)
    union = set_a | set_b
    inter = set_a & set_b
    if not union:
        return 0.0
    w_inter = sum(weights.get(t, 1.0) for t in inter)
    w_union = sum(weights.get(t, 1.0) for t in union)
    return w_inter / w_union if w_union > 0 else 0.0

def time_similarity_bonus(wa, sa, wb, sb) -> float:
    """기상/취침 시간 차이가 1시간 이내이면 각각 +5% 보정"""
    bonus = 0.0
    if wa is not None and wb is not None and abs(wa - wb) <= 1:
        bonus += 0.05
    if sa is not None and sb is not None and abs(sa - sb) <= 1:
        bonus += 0.05
    return bonus

def primary_filter(me: dict, candidate: dict) -> bool:
    """절대 조건 불일치 시 False 반환. 성별과 흡연 여부는 반드시 같은 사람끼리만 매칭한다."""
    # 기숙사 룸메이트 전제를 반영하여 남자는 남자끼리, 여자는 여자끼리만 매칭
    if me.get("gender") and candidate.get("gender") and me.get("gender") != candidate.get("gender"):
        return False

    # 흡연자는 흡연자끼리, 비흡연자는 비흡연자끼리만 매칭
    if me.get("smoke") and candidate.get("smoke") and me.get("smoke") != candidate.get("smoke"):
        return False

    return True

# ══════════════════════════════════════════════════════════════════════════════
# Pydantic 모델
# ══════════════════════════════════════════════════════════════════════════════

class MealApply(BaseModel):
    studentId: str
    name: str
    menu: str
    # v6부터는 연락수단을 종류와 세부사항으로 분리한다. contact는 기존 버전 호환용이다.
    contactType: Optional[str] = ""
    contactDetail: Optional[str] = ""
    contact: Optional[str] = ""

class MealRequestCreate(BaseModel):
    targetStudentId: str
    requesterStudentId: str
    requesterName: str
    # 요청자의 연락처는 받지 않는다. 필요한 말은 message에 자유롭게 작성한다.
    requesterContact: Optional[str] = ""  # 기존 프론트 호환용, 저장하지 않음
    message: Optional[str] = ""

class MealRequestRespond(BaseModel):
    status: str

class UserRegister(BaseModel):
    sid: str
    name: str
    dept: Optional[str] = ""
    gender: Optional[str] = ""
    smoke: Optional[str] = ""
    contactType: Optional[str] = ""
    contactDetail: Optional[str] = ""
    contact: Optional[str] = ""  # 이전/임시 프론트 호환용
    traits: List[str]
    pf_gender: Optional[str] = "any"
    pf_smoke: Optional[str] = "any"
    wake_time: Optional[int] = None
    sleep_time: Optional[int] = None

class RoommateRequestCreate(BaseModel):
    targetSid: str
    requesterSid: str
    requesterName: str
    message: Optional[str] = ""

class RoommateRequestRespond(BaseModel):
    status: str

class RoommateCancelRequest(BaseModel):
    sid: str

class RoommateCancelRespond(BaseModel):
    sid: str
    action: str

class WeightsUpdate(BaseModel):
    weights: Dict[str, float]

# ══════════════════════════════════════════════════════════════════════════════
# 시작 시 DB 초기화 + 해시 테이블 복원
# ══════════════════════════════════════════════════════════════════════════════

@app.on_event("startup")
def startup_event():
    init_db()
    load_weights_from_db()
    rebuild_hash_table_from_db()

# 테스트/직접 실행 시에도 안전하게 테이블 생성
init_db()
load_weights_from_db()
rebuild_hash_table_from_db()

# ══════════════════════════════════════════════════════════════════════════════
# 밥약 API
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/apply")
def meal_apply(body: MealApply):
    student_id = body.studentId.strip()
    name = body.name.strip()
    menu = body.menu.strip()
    contact_type = (body.contactType or "").strip()
    contact_detail = (body.contactDetail or body.contact or "").strip()
    contact = f"{contact_type}: {contact_detail}" if contact_type and contact_detail else contact_detail

    if not student_id or not name or not menu:
        raise HTTPException(status_code=400, detail="학번, 이름, 메뉴를 모두 입력해주세요.")
    if not contact_type or not contact_detail:
        raise HTTPException(status_code=400, detail="연락수단과 세부사항을 모두 입력해주세요.")

    try:
        with get_conn() as conn:
            conn.execute(
                "INSERT INTO meals (studentId, name, menu, contact, contact_type, contact_detail) VALUES (?, ?, ?, ?, ?, ?)",
                (student_id, name, menu, contact, contact_type, contact_detail),
            )
            conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="이미 신청한 학번입니다.")

    return {"message": f"{name}님의 밥약 신청이 완료되었습니다!"}

@app.get("/api/list")
def meal_list_get():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT m.*,
                   COALESCE((
                       SELECT COUNT(*)
                       FROM meal_requests r
                       WHERE r.target_student_id = m.studentId
                         AND r.status = 'accepted'
                   ), 0) AS planned_count
            FROM meals m
            ORDER BY m.created_at DESC
        """).fetchall()
    return [row_to_meal(row) for row in rows]

@app.delete("/api/meal/{student_id}")
def delete_meal(student_id: str, ownerStudentId: str = ""):
    student_id = student_id.strip()
    owner_id = ownerStudentId.strip()

    # 간단한 권한 확인: 요청함에서 본인 학번으로 확인한 사용자만 본인 신청을 삭제할 수 있게 함
    # 실제 서비스에서는 로그인/세션 인증으로 대체하는 것이 더 안전하다.
    if owner_id != student_id:
        raise HTTPException(status_code=403, detail="본인 학번으로 요청함을 확인한 경우에만 삭제할 수 있습니다.")

    with get_conn() as conn:
        conn.execute("DELETE FROM meal_requests WHERE target_student_id = ? OR requester_student_id = ?", (student_id, student_id))
        cur = conn.execute("DELETE FROM meals WHERE studentId = ?", (student_id,))
        conn.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="해당 학번의 신청이 없습니다.")
    return {"message": f"{student_id} 밥약 신청 삭제 완료"}


@app.post("/api/meal/request")
def create_meal_request(body: MealRequestCreate):
    target_id = body.targetStudentId.strip()
    requester_id = body.requesterStudentId.strip()
    requester_name = body.requesterName.strip()
    message = (body.message or "").strip()

    if not target_id or not requester_id or not requester_name:
        raise HTTPException(status_code=400, detail="요청자 학번, 이름, 대상 학번을 입력해주세요.")
    if target_id == requester_id:
        raise HTTPException(status_code=400, detail="본인에게는 밥약 요청을 보낼 수 없습니다.")

    with get_conn() as conn:
        target = conn.execute("SELECT * FROM meals WHERE studentId = ?", (target_id,)).fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="대상 밥약 신청을 찾을 수 없습니다.")
        duplicate = conn.execute(
            "SELECT id FROM meal_requests WHERE target_student_id = ? AND requester_student_id = ? AND status = 'pending'",
            (target_id, requester_id),
        ).fetchone()
        if duplicate:
            raise HTTPException(status_code=409, detail="이미 대기 중인 요청이 있습니다.")
        req_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO meal_requests
            (id, target_student_id, requester_student_id, requester_name, message, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
            """,
            (req_id, target_id, requester_id, requester_name, message),
        )
        conn.commit()
    return {"message": "밥약 요청을 보냈습니다.", "id": req_id}

@app.get("/api/meal/requests/{student_id}")
def get_meal_requests(student_id: str):
    with get_conn() as conn:
        received_rows = conn.execute(
            """
            SELECT r.*,
                   m.name AS target_name,
                   m.menu AS target_menu,
                   m.contact AS target_contact,
                   m.contact_type AS target_contact_type,
                   m.contact_detail AS target_contact_detail
            FROM meal_requests r
            JOIN meals m ON r.target_student_id = m.studentId
            WHERE r.target_student_id = ?
            ORDER BY r.created_at DESC
            """,
            (student_id,),
        ).fetchall()

        sent_rows = conn.execute(
            """
            SELECT r.*,
                   m.name AS target_name,
                   m.menu AS target_menu,
                   m.contact AS target_contact,
                   m.contact_type AS target_contact_type,
                   m.contact_detail AS target_contact_detail
            FROM meal_requests r
            JOIN meals m ON r.target_student_id = m.studentId
            WHERE r.requester_student_id = ?
            ORDER BY r.created_at DESC
            """,
            (student_id,),
        ).fetchall()

    def row_get(row, key, default=""):
        return row[key] if key in row.keys() and row[key] is not None else default

    def pack(row, mode):
        status = row_get(row, "status", "pending")
        target_contact_type = row_get(row, "target_contact_type", "기타") or "기타"
        target_contact_detail = row_get(row, "target_contact_detail", "") or row_get(row, "target_contact", "")

        item = {
            "id": row_get(row, "id"),
            "targetSid": row_get(row, "target_student_id"),
            "targetName": row_get(row, "target_name"),
            "targetMenu": row_get(row, "target_menu"),
            "targetDept": "",
            "requesterSid": row_get(row, "requester_student_id"),
            "requesterName": row_get(row, "requester_name"),
            "requesterDept": "",
            "message": row_get(row, "message"),
            "status": status,
            "created_at": row_get(row, "created_at"),
        }

        if mode == "sent":
            item["counterpartName"] = item["targetName"]
            item["counterpartDept"] = item["targetDept"]
            item["counterpartContactType"] = target_contact_type
            item["counterpartContactDetail"] = target_contact_detail
        else:
            item["counterpartName"] = item["requesterName"]
            item["counterpartDept"] = item["requesterDept"]
            # 밥약 요청자는 별도 연락수단을 받지 않으므로 받은 요청에서는 연락처를 비워둔다.
            item["counterpartContactType"] = ""
            item["counterpartContactDetail"] = ""

        # 수락된 보낸 요청에서만 상대 연락수단을 보여준다.
        if mode == "sent" and status == "accepted":
            item["contactType"] = item["counterpartContactType"]
            item["contactDetail"] = item["counterpartContactDetail"]
            item["contact"] = (
                f"{item['contactType']}: {item['contactDetail']}"
                if item["contactDetail"]
                else ""
            )

        return item

    return {
        "received": [pack(r, "received") for r in received_rows],
        "sent": [pack(r, "sent") for r in sent_rows],
    }

@app.post("/api/meal/requests/{request_id}/respond")
def respond_meal_request(request_id: str, body: MealRequestRespond):
    status = body.status.strip().lower()
    if status not in {"accepted", "rejected"}:
        raise HTTPException(status_code=400, detail="status는 accepted 또는 rejected만 가능합니다.")

    with get_conn() as conn:
        row = conn.execute("SELECT * FROM meal_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="밥약 요청을 찾을 수 없습니다.")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail="대기 중인 요청만 수락 또는 거절할 수 있습니다.")

        conn.execute(
            "UPDATE meal_requests SET status = ? WHERE id = ?",
            (status, request_id),
        )
        conn.commit()

    return {
        "message": "요청을 수락했습니다." if status == "accepted" else "요청을 거절했습니다.",
        "status": status,
    }


@app.post("/api/roommate/requests/{request_id}/respond")
def respond_roommate_request(request_id: str, body: RoommateRequestRespond):
    status = body.status.strip().lower()
    if status not in {"accepted", "rejected"}:
        raise HTTPException(status_code=400, detail="status는 accepted 또는 rejected만 가능합니다.")
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail="대기 중인 요청만 수락 또는 거절할 수 있습니다.")

        if status == "accepted":
            requester_sid = row["requester_sid"]
            target_sid = row["target_sid"]
            if _roommate_active_relation(conn, requester_sid) or _roommate_active_relation(conn, target_sid):
                raise HTTPException(status_code=409, detail="이미 룸메 예정 상태인 사용자가 포함되어 있어 수락할 수 없습니다.")
            # 한 요청이 수락되면 두 사용자와 관련된 다른 pending 요청은 자동 취소한다.
            conn.execute(
                """
                UPDATE roommate_requests
                SET status = 'auto_cancelled'
                WHERE status = 'pending'
                  AND id != ?
                  AND (requester_sid IN (?, ?) OR target_sid IN (?, ?))
                """,
                (request_id, requester_sid, target_sid, requester_sid, target_sid),
            )

        conn.execute(
            "UPDATE roommate_requests SET status = ?, cancel_requested_by = '' WHERE id = ?",
            (status, request_id),
        )
        conn.commit()
    return {"message": "accepted" if status == "accepted" else "rejected"}

@app.post("/api/roommate/requests/{request_id}/cancel-pending")
def cancel_pending_roommate_request(request_id: str, body: RoommateCancelRequest):
    sid = body.sid.strip()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if row["requester_sid"] != sid:
            raise HTTPException(status_code=403, detail="신청자 본인만 보낸 요청을 취소할 수 있습니다.")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail="대기 중인 요청만 취소할 수 있습니다.")
        conn.execute("UPDATE roommate_requests SET status = 'cancelled' WHERE id = ?", (request_id,))
        conn.commit()
    return {"message": "룸메 신청을 취소했습니다."}

@app.post("/api/roommate/requests/{request_id}/cancel-request")
def request_cancel_accepted_roommate(request_id: str, body: RoommateCancelRequest):
    sid = body.sid.strip()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if sid not in {row["requester_sid"], row["target_sid"]}:
            raise HTTPException(status_code=403, detail="해당 룸메 예정 관계의 당사자만 취소 요청을 보낼 수 있습니다.")
        if row["status"] != "accepted":
            raise HTTPException(status_code=409, detail="수락된 룸메 예정 상태에서만 취소 요청을 보낼 수 있습니다.")
        conn.execute(
            "UPDATE roommate_requests SET status = 'cancel_requested', cancel_requested_by = ? WHERE id = ?",
            (sid, request_id),
        )
        conn.commit()
    return {"message": "취소 요청을 보냈습니다. 상대가 동의하면 합의 취소됩니다."}

@app.post("/api/roommate/requests/{request_id}/cancel-respond")
def respond_cancel_roommate_request(request_id: str, body: RoommateCancelRespond):
    sid = body.sid.strip()
    action = body.action.strip().lower()
    if action not in {"agree", "keep"}:
        raise HTTPException(status_code=400, detail="action은 agree 또는 keep만 가능합니다.")
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if row["status"] != "cancel_requested":
            raise HTTPException(status_code=409, detail="취소 요청 중인 상태만 처리할 수 있습니다.")
        if sid not in {row["requester_sid"], row["target_sid"]}:
            raise HTTPException(status_code=403, detail="해당 룸메 예정 관계의 당사자만 처리할 수 있습니다.")
        if row["cancel_requested_by"] == sid:
            raise HTTPException(status_code=403, detail="취소 요청을 보낸 사람은 직접 동의/유지를 처리할 수 없습니다.")
        new_status = "cancelled_agreed" if action == "agree" else "accepted"
        conn.execute(
            "UPDATE roommate_requests SET status = ?, cancel_requested_by = '' WHERE id = ?",
            (new_status, request_id),
        )
        conn.commit()
    return {"message": "합의 취소가 완료되었습니다." if action == "agree" else "룸메 예정 상태를 유지합니다."}


# ══════════════════════════════════════════════════════════════════════════════
# 룸메이트 매칭 API
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/meta/traits")
def get_traits_meta():
    load_weights_from_db()
    return {"traits": [{**t, "weight": weights.get(t["key"], t["weight"])} for t in TRAITS_META]}

@app.put("/weights")
def update_weights(body: WeightsUpdate):
    with get_conn() as conn:
        for k, v in body.weights.items():
            if k in weights:
                new_weight = round(max(1.0, min(2.0, float(v))), 1)
                weights[k] = new_weight
                conn.execute(
                    "INSERT INTO trait_weights (trait_key, weight) VALUES (?, ?) "
                    "ON CONFLICT(trait_key) DO UPDATE SET weight = excluded.weight",
                    (k, new_weight),
                )
        conn.commit()
    cache_flush()
    return {"message": "가중치 업데이트 완료"}

@app.post("/users")
def register_user(body: UserRegister):
    user = {
        "sid": body.sid.strip(),
        "name": body.name.strip(),
        "dept": body.dept or "",
        "gender": body.gender or "",
        "smoke": body.smoke or "",
        "contact_type": (body.contactType or "").strip(),
        "contact_detail": (body.contactDetail or body.contact or "").strip(),
        "traits": [t for t in body.traits if t in weights],
        "pf_gender": "any",
        "pf_smoke": "any",
        "wake_time": body.wake_time,
        "sleep_time": body.sleep_time,
    }

    if not user["sid"] or not user["name"]:
        raise HTTPException(status_code=400, detail="학번과 이름을 입력해주세요.")
    if len(user["sid"]) != 10:
        raise HTTPException(status_code=400, detail="학번 10자리를 입력해주세요.")
    if user["gender"] not in {"남", "여"}:
        raise HTTPException(status_code=400, detail="본인의 성별을 선택해주세요.")
    if user["smoke"] not in {"yes", "no"}:
        raise HTTPException(status_code=400, detail="본인의 흡연 여부를 선택해주세요.")
    if not user["contact_type"] or not user["contact_detail"]:
        raise HTTPException(status_code=400, detail="연락수단과 세부사항을 모두 입력해주세요.")

    try:
        slot = ht_insert(user)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    try:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO users
                (sid, name, dept, gender, smoke, contact_type, contact_detail, traits, pf_gender, pf_smoke, wake_time, sleep_time, slot, chain_index)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user["sid"], user["name"], user["dept"], user["gender"], user["smoke"],
                    user["contact_type"], user["contact_detail"],
                    json.dumps(user["traits"], ensure_ascii=False),
                    user["pf_gender"], user["pf_smoke"], user["wake_time"], user["sleep_time"],
                    user["slot"], user["chain_index"],
                ),
            )
            conn.commit()
    except sqlite3.IntegrityError:
        rebuild_hash_table_from_db()
        raise HTTPException(status_code=409, detail="이미 등록된 학번입니다.")

    cache_flush()
    return {"message": f"{body.name}님이 등록되었습니다", "slot": slot, "partition": get_partition(slot)}

@app.get("/match/{sid}")
def match_user(sid: str):
    start = time.time()
    me, my_slot = ht_find(sid)
    if not me:
        raise HTTPException(status_code=404, detail="등록된 학번이 아닙니다. 먼저 등록해주세요.")

    cache_key = f"match:{sid}"
    cached = cache_get(cache_key)
    if cached:
        result = dict(cached)
        result["elapsed_ms"] = round((time.time() - start) * 1000, 1)
        result["cache_status"] = "HIT"
        return result

    results = []
    for candidate in ht_all_users():
        if candidate["sid"] == sid:
            continue
        if not primary_filter(me, candidate):
            continue

        score = weighted_jaccard(me["traits"], candidate["traits"])
        bonus = time_similarity_bonus(
            me.get("wake_time"), me.get("sleep_time"),
            candidate.get("wake_time"), candidate.get("sleep_time"),
        )
        score = min(1.0, score + bonus)

        set_me = set(me["traits"])
        set_cd = set(candidate["traits"])
        shared = list(set_me & set_cd)
        heavy = [t for t in shared if weights.get(t, 1.0) >= 1.2]
        unique = list(set_cd - set_me)

        results.append({
            "sid": candidate["sid"],
            "name": candidate["name"],
            "dept": candidate.get("dept", ""),
            "gender": candidate.get("gender", ""),
            "smoke": candidate.get("smoke", ""),
            "slot": candidate["slot"],
            "score": round(score, 4),
            "score_pct": round(score * 100, 1),
            "shared_traits": shared,
            "heavy_shared": heavy,
            "unique_traits": unique,
            "wake_time": candidate.get("wake_time"),
            "sleep_time": candidate.get("sleep_time"),
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    response = {
        "me": {"name": me["name"], "sid": me["sid"]},
        "slot": my_slot,
        "results": results,
        "cache_status": "MISS",
        "elapsed_ms": round((time.time() - start) * 1000, 1),
    }
    cache_set(cache_key, response)
    return response

@app.get("/table")
def get_table():
    total = sum(len(chain) for chain in hash_table)
    used = sum(1 for chain in hash_table if chain)
    load = round(used / TABLE_SIZE * 100, 1)
    slots = []
    for i, chain in enumerate(hash_table):
        if chain:
            slots.append({
                "slot": i,
                "partition": get_partition(i),
                "count": len(chain),
                "users": [
                    {
                        "sid": u["sid"],
                        "name": u["name"],
                        "dept": u.get("dept", ""),
                        "gender": u.get("gender", ""),
                        "smoke": u.get("smoke", ""),
                        "chain_index": u["chain_index"],
                    }
                    for u in chain
                ],
            })
    return {"total_users": total, "used_slots": used, "load_factor": load, "slots": slots}


def _roommate_active_relation(conn, sid: str):
    return conn.execute(
        """
        SELECT * FROM roommate_requests
        WHERE status IN ('accepted', 'cancel_requested')
          AND (requester_sid = ? OR target_sid = ?)
        LIMIT 1
        """,
        (sid, sid),
    ).fetchone()


def _roommate_pair_request(conn, a: str, b: str, statuses: tuple):
    placeholders = ",".join("?" for _ in statuses)
    return conn.execute(
        f"""
        SELECT * FROM roommate_requests
        WHERE status IN ({placeholders})
          AND (
            (requester_sid = ? AND target_sid = ?)
            OR
            (requester_sid = ? AND target_sid = ?)
          )
        LIMIT 1
        """,
        (*statuses, a, b, b, a),
    ).fetchone()

@app.post("/api/roommate/request")
def create_roommate_request(body: RoommateRequestCreate):
    target_sid = body.targetSid.strip()
    requester_sid = body.requesterSid.strip()
    requester_name = body.requesterName.strip()
    message = (body.message or "").strip()

    if not target_sid or not requester_sid or not requester_name:
        raise HTTPException(status_code=400, detail="요청자 학번, 이름, 대상 학번을 입력해주세요.")
    if target_sid == requester_sid:
        raise HTTPException(status_code=400, detail="본인에게는 룸메 신청을 보낼 수 없습니다.")

    with get_conn() as conn:
        target = conn.execute("SELECT * FROM users WHERE sid = ?", (target_sid,)).fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="대상 룸메이트 등록 정보를 찾을 수 없습니다.")
        requester = conn.execute("SELECT * FROM users WHERE sid = ?", (requester_sid,)).fetchone()
        if not requester:
            raise HTTPException(status_code=404, detail="요청자 학번이 룸메이트 시스템에 등록되어 있지 않습니다.")

        if _roommate_active_relation(conn, requester_sid):
            raise HTTPException(status_code=409, detail="이미 룸메 예정 상태이거나 취소 합의 진행 중이라 새 신청을 보낼 수 없습니다.")
        if _roommate_active_relation(conn, target_sid):
            raise HTTPException(status_code=409, detail="상대가 이미 룸메 예정 상태이거나 취소 합의 진행 중이라 신청할 수 없습니다.")

        pending_pair = _roommate_pair_request(conn, requester_sid, target_sid, ("pending",))
        if pending_pair:
            raise HTTPException(status_code=409, detail="이미 두 사람 사이에 대기 중인 룸메 신청이 있습니다. 받은 요청함에서 수락/거절을 처리해주세요.")

        req_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO roommate_requests
            (id, target_sid, requester_sid, requester_name, message, status, cancel_requested_by)
            VALUES (?, ?, ?, ?, ?, 'pending', '')
            """,
            (req_id, target_sid, requester_sid, requester_name, message),
        )
        conn.commit()
    return {"message": "룸메 신청을 보냈습니다.", "id": req_id}

@app.get("/api/roommate/requests/{sid}")
def get_roommate_requests(sid: str):
    sid = sid.strip()
    with get_conn() as conn:
        received_rows = conn.execute(
            """
            SELECT r.*, target.name AS target_name, target.dept AS target_dept,
                   target.contact_type AS target_contact_type, target.contact_detail AS target_contact_detail,
                   requester.name AS requester_real_name, requester.dept AS requester_dept,
                   requester.contact_type AS requester_contact_type, requester.contact_detail AS requester_contact_detail
            FROM roommate_requests r
            JOIN users target ON r.target_sid = target.sid
            JOIN users requester ON r.requester_sid = requester.sid
            WHERE r.target_sid = ?
            ORDER BY r.created_at DESC
            """,
            (sid,),
        ).fetchall()
        sent_rows = conn.execute(
            """
            SELECT r.*, target.name AS target_name, target.dept AS target_dept,
                   target.contact_type AS target_contact_type, target.contact_detail AS target_contact_detail,
                   requester.name AS requester_real_name, requester.dept AS requester_dept,
                   requester.contact_type AS requester_contact_type, requester.contact_detail AS requester_contact_detail
            FROM roommate_requests r
            JOIN users target ON r.target_sid = target.sid
            JOIN users requester ON r.requester_sid = requester.sid
            WHERE r.requester_sid = ?
            ORDER BY r.created_at DESC
            """,
            (sid,),
        ).fetchall()

    def pack(row, mode):
        item = {
            "id": row["id"],
            "targetSid": row["target_sid"],
            "targetName": row["target_name"],
            "targetDept": row["target_dept"],
            "requesterSid": row["requester_sid"],
            "requesterName": row["requester_name"] or row["requester_real_name"],
            "requesterDept": row["requester_dept"],
            "message": row["message"],
            "status": row["status"],
            "cancelRequestedBy": row["cancel_requested_by"] if "cancel_requested_by" in row.keys() else "",
            "created_at": row["created_at"],
        }
        if mode == "sent":
            item["counterpartName"] = row["target_name"]
            item["counterpartDept"] = row["target_dept"]
            item["counterpartContactType"] = row["target_contact_type"] or "기타"
            item["counterpartContactDetail"] = row["target_contact_detail"] or ""
        else:
            item["counterpartName"] = row["requester_name"] or row["requester_real_name"]
            item["counterpartDept"] = row["requester_dept"]
            item["counterpartContactType"] = row["requester_contact_type"] or "기타"
            item["counterpartContactDetail"] = row["requester_contact_detail"] or ""

        if row["status"] in {"accepted", "cancel_requested"}:
            item["contactType"] = item["counterpartContactType"]
            item["contactDetail"] = item["counterpartContactDetail"]
            item["contact"] = (f"{item['contactType']}: {item['contactDetail']}" if item["contactDetail"] else "")
        return item

    return {"received": [pack(r, "received") for r in received_rows], "sent": [pack(r, "sent") for r in sent_rows]}

@app.post("/api/roommate/requests/{request_id}/respond")
def respond_roommate_request(request_id: str, body: RoommateRequestRespond):
    status = body.status.strip().lower()
    if status not in {"accepted", "rejected"}:
        raise HTTPException(status_code=400, detail="status는 accepted 또는 rejected만 가능합니다.")
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail="대기 중인 요청만 수락 또는 거절할 수 있습니다.")

        if status == "accepted":
            requester_sid = row["requester_sid"]
            target_sid = row["target_sid"]
            if _roommate_active_relation(conn, requester_sid) or _roommate_active_relation(conn, target_sid):
                raise HTTPException(status_code=409, detail="이미 룸메 예정 상태인 사용자가 포함되어 있어 수락할 수 없습니다.")
            conn.execute(
                """
                UPDATE roommate_requests
                SET status = 'auto_cancelled'
                WHERE status = 'pending'
                  AND id != ?
                  AND (requester_sid IN (?, ?) OR target_sid IN (?, ?))
                """,
                (request_id, requester_sid, target_sid, requester_sid, target_sid),
            )

        conn.execute(
            "UPDATE roommate_requests SET status = ?, cancel_requested_by = '' WHERE id = ?",
            (status, request_id),
        )
        conn.commit()
    return {"message": "accepted" if status == "accepted" else "rejected"}

@app.post("/api/roommate/requests/{request_id}/cancel-pending")
def cancel_pending_roommate_request(request_id: str, body: RoommateCancelRequest):
    sid = body.sid.strip()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if row["requester_sid"] != sid:
            raise HTTPException(status_code=403, detail="신청자 본인만 보낸 요청을 취소할 수 있습니다.")
        if row["status"] != "pending":
            raise HTTPException(status_code=409, detail="대기 중인 요청만 취소할 수 있습니다.")
        conn.execute("UPDATE roommate_requests SET status = 'cancelled' WHERE id = ?", (request_id,))
        conn.commit()
    return {"message": "룸메 신청을 취소했습니다."}

@app.post("/api/roommate/requests/{request_id}/cancel-request")
def request_cancel_accepted_roommate(request_id: str, body: RoommateCancelRequest):
    sid = body.sid.strip()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if sid not in {row["requester_sid"], row["target_sid"]}:
            raise HTTPException(status_code=403, detail="해당 룸메 예정 관계의 당사자만 취소 요청을 보낼 수 있습니다.")
        if row["status"] != "accepted":
            raise HTTPException(status_code=409, detail="수락된 룸메 예정 상태에서만 취소 요청을 보낼 수 있습니다.")
        conn.execute(
            "UPDATE roommate_requests SET status = 'cancel_requested', cancel_requested_by = ? WHERE id = ?",
            (sid, request_id),
        )
        conn.commit()
    return {"message": "취소 요청을 보냈습니다. 상대가 동의하면 합의 취소됩니다."}

@app.post("/api/roommate/requests/{request_id}/cancel-respond")
def respond_cancel_roommate_request(request_id: str, body: RoommateCancelRespond):
    sid = body.sid.strip()
    action = body.action.strip().lower()
    if action not in {"agree", "keep"}:
        raise HTTPException(status_code=400, detail="action은 agree 또는 keep만 가능합니다.")
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM roommate_requests WHERE id = ?", (request_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="룸메 요청을 찾을 수 없습니다.")
        if row["status"] != "cancel_requested":
            raise HTTPException(status_code=409, detail="취소 요청 중인 상태만 처리할 수 있습니다.")
        if sid not in {row["requester_sid"], row["target_sid"]}:
            raise HTTPException(status_code=403, detail="해당 룸메 예정 관계의 당사자만 처리할 수 있습니다.")
        if row["cancel_requested_by"] == sid:
            raise HTTPException(status_code=403, detail="취소 요청을 보낸 사람은 직접 동의/유지를 처리할 수 없습니다.")
        new_status = "cancelled_agreed" if action == "agree" else "accepted"
        conn.execute(
            "UPDATE roommate_requests SET status = ?, cancel_requested_by = '' WHERE id = ?",
            (new_status, request_id),
        )
        conn.commit()
    return {"message": "합의 취소가 완료되었습니다." if action == "agree" else "룸메 예정 상태를 유지합니다."}

@app.delete("/users/{sid}")
def delete_user(sid: str, confirmSid: str = ""):
    sid = sid.strip()
    confirm_sid = confirmSid.strip()
    if confirm_sid != sid:
        raise HTTPException(status_code=403, detail="삭제하려는 등록 정보와 입력한 학번이 일치해야 삭제할 수 있습니다.")

    slot = hash_func(sid)
    chain = hash_table[slot]
    for i, u in enumerate(chain):
        if u["sid"] == sid:
            chain.pop(i)
            with get_conn() as conn:
                conn.execute("DELETE FROM roommate_requests WHERE target_sid = ? OR requester_sid = ?", (sid, sid))
                conn.execute("DELETE FROM users WHERE sid = ?", (sid,))
                conn.commit()
            for j, u2 in enumerate(chain):
                u2["chain_index"] = j
            sync_user_positions_to_db()
            cache_flush()
            return {"message": f"{sid} 삭제 완료"}
    raise HTTPException(status_code=404, detail="해당 학번이 존재하지 않습니다.")

@app.get("/cache")
def get_cache_view():
    now = time.time()
    with _cache_lock:
        expired = [k for k, v in _cache.items() if v["expires_at"] <= now]
        for k in expired:
            del _cache[k]
        items = []
        for k, v in _cache.items():
            data = v["data"]
            count = len(data.get("results", [])) if isinstance(data, dict) else 0
            items.append({"key": k, "count": count, "ttl": round(v["expires_at"] - now, 1)})
    return {"size": len(items), "items": items}

@app.delete("/cache")
def flush_cache_endpoint():
    cache_flush()
    return {"message": "캐시가 초기화되었습니다"}

@app.delete("/users/{sid}")
def delete_user(sid: str, confirmSid: str = ""):
    sid = sid.strip()
    confirm_sid = confirmSid.strip()
    if confirm_sid != sid:
        raise HTTPException(status_code=403, detail="삭제하려는 등록 정보와 입력한 학번이 일치해야 삭제할 수 있습니다.")

    slot = hash_func(sid)
    chain = hash_table[slot]
    for i, u in enumerate(chain):
        if u["sid"] == sid:
            chain.pop(i)
            with get_conn() as conn:
                conn.execute("DELETE FROM roommate_requests WHERE target_sid = ? OR requester_sid = ?", (sid, sid))
                conn.execute("DELETE FROM users WHERE sid = ?", (sid,))
                conn.commit()
            for j, u2 in enumerate(chain):
                u2["chain_index"] = j
            sync_user_positions_to_db()
            cache_flush()
            return {"message": f"{sid} 삭제 완료"}
    raise HTTPException(status_code=404, detail="해당 학번이 존재하지 않습니다.")

# ══════════════════════════════════════════════════════════════════════════════
# 갤러리 API
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/gallery")
async def gallery_upload(
    name: str = Form(...),
    menu: str = Form(...),
    tags: str = Form(""),
    image: UploadFile = File(...),
):
    ext = os.path.splitext(image.filename or "")[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="jpg/png/gif/webp 파일만 업로드 가능합니다.")

    contents = await image.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="파일 크기는 10MB 이하여야 합니다.")

    filename = f"{uuid.uuid4().hex}{ext}"
    save_path = os.path.join(UPLOADS_DIR, filename)
    with open(save_path, "wb") as f:
        f.write(contents)

    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    post_id = str(uuid.uuid4())
    image_url = f"/uploads/{filename}"

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO gallery_posts (id, name, menu, tags, image_url, yummy, where_count)
            VALUES (?, ?, ?, ?, ?, 0, 0)
            """,
            (post_id, name.strip(), menu.strip(), json.dumps(tag_list, ensure_ascii=False), image_url),
        )
        row = conn.execute("SELECT * FROM gallery_posts WHERE id = ?", (post_id,)).fetchone()
        conn.commit()

    post = row_to_post(row)
    return {"message": "업로드 완료!", "post": post}

@app.get("/api/gallery")
def gallery_list():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM gallery_posts ORDER BY created_at DESC").fetchall()
    return [row_to_post(row) for row in rows]

@app.delete("/api/gallery/{post_id}")
def gallery_delete(post_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM gallery_posts WHERE id = ?", (post_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="게시글이 없습니다.")

        filename = row["image_url"].split("/")[-1]
        file_path = os.path.join(UPLOADS_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)

        conn.execute("DELETE FROM gallery_posts WHERE id = ?", (post_id,))
        conn.commit()
    return {"message": "삭제 완료"}

@app.post("/api/gallery/{post_id}/react")
def gallery_react(post_id: str, reaction: str):
    if reaction not in {"yummy", "where"}:
        raise HTTPException(status_code=400, detail="잘못된 리액션입니다.")

    column = "yummy" if reaction == "yummy" else "where_count"
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM gallery_posts WHERE id = ?", (post_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="게시글이 없습니다.")
        conn.execute(f"UPDATE gallery_posts SET {column} = {column} + 1 WHERE id = ?", (post_id,))
        updated = conn.execute("SELECT * FROM gallery_posts WHERE id = ?", (post_id,)).fetchone()
        conn.commit()

    return {"reactions": row_to_post(updated)["reactions"]}

@app.get("/hash/{sid}")
def get_hash(sid: str):
    h = 0
    steps = []
    for i, ch in enumerate(sid):
        h = (h * HASH_MULTIPLIER + ord(ch)) % TABLE_SIZE
        steps.append({"index": i, "char": ch, "code": ord(ch), "h": h})
    slot = h
    return {"sid": sid, "slot": slot, "partition": get_partition(slot), "steps": steps}

# ══════════════════════════════════════════════════════════════════════════════
# 정적 파일 서빙 — API 라우트보다 반드시 나중에 마운트해야 우선순위가 올바름
# ══════════════════════════════════════════════════════════════════════════════
app.mount("/uploads", StaticFiles(directory=UPLOADS_DIR), name="uploads")
app.mount("/", StaticFiles(directory=BASE_DIR, html=True), name="static")
