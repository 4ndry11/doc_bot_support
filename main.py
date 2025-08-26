# -*- coding: utf-8 -*-
from __future__ import annotations

import os, sys, json, re, logging, unicodedata
from pathlib import Path
from typing import Optional, Tuple, Dict, List
from datetime import datetime, timezone, timedelta

import requests
from telegram import Update
from telegram.ext import Updater, MessageHandler, CommandHandler, Filters, CallbackContext
from telegram.error import Conflict as TgConflict

# ========= ТОЛЬКО эти читаем из окружения =========
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "").strip()     # например: https://ua.zvilnymo.com.ua/rest/596/xxx/
BITRIX_CONTACT_URL = os.getenv("BITRIX_CONTACT_URL", "").strip()      # можно задать полную ссылку crm.contact.list.json
DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "/etc/secrets/main_acc.json").strip()

# ========= Остальное — фиксируем в коде =========
CATEGORY_ID = 1
CONSULTANT_FIELD = "UF_CRM_1708783848"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ========= Google API =========
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

# ========= Логи =========
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("bot")
logging.getLogger("apscheduler").setLevel(logging.WARNING)

# ========= Проверка обязательных env =========
def _assert_required_env():
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not (BITRIX_WEBHOOK_BASE or BITRIX_CONTACT_URL):
        missing.append("BITRIX_WEBHOOK_BASE or BITRIX_CONTACT_URL")
    if not DRIVE_ROOT_FOLDER_ID:
        missing.append("DRIVE_ROOT_FOLDER_ID")
    if missing:
        raise RuntimeError("Не заданы переменные окружения: " + ", ".join(missing))

# ========= HTTP с таймаутами =========
def http_get(url: str, **kwargs):
    kwargs.setdefault("timeout", 30)
    return requests.get(url, **kwargs)

def http_post(url: str, **kwargs):
    kwargs.setdefault("timeout", 30)
    return requests.post(url, **kwargs)

# ========= Drive / креды =========
SA_EMAIL: Optional[str] = None

def build_drive():
    """
    Берём ключ сервис-аккаунта из файла:
    1) GOOGLE_SERVICE_ACCOUNT_FILE (ENV), иначе
    2) /etc/secrets/main_acc.json (дефолт), иначе
    3) main_acc.json рядом со скриптом (локальная отладка)
    """
    global SA_EMAIL

    # 1) ENV/дефолт
    creds_path = GOOGLE_SERVICE_ACCOUNT_FILE
    if os.path.exists(creds_path):
        try:
            creds = Credentials.from_service_account_file(creds_path, scopes=DRIVE_SCOPES)
            SA_EMAIL = getattr(creds, "service_account_email", None)
            log.info("[drive] using SA %s (file=%s)", SA_EMAIL, creds_path)
            return build("drive", "v3", credentials=creds, cache_discovery=False)
        except Exception as e:
            raise RuntimeError(f"Не удалось прочитать ключ из {creds_path}: {e}")

    # 2) Локальный fallback
    script_dir = Path(sys.modules["__main__"].__file__).resolve().parent if hasattr(sys.modules.get("__main__"), "__file__") else Path.cwd()
    candidate = (script_dir / "main_acc.json").resolve()
    if candidate.exists():
        try:
            creds = Credentials.from_service_account_file(str(candidate), scopes=DRIVE_SCOPES)
            SA_EMAIL = getattr(creds, "service_account_email", None)
            log.info("[drive] using SA %s (file=%s)", SA_EMAIL, candidate)
            return build("drive", "v3", credentials=creds, cache_discovery=False)
        except Exception as e:
            raise RuntimeError(f"Не удалось прочитать ключ из {candidate}: {e}")

    # 3) Ничего не нашли
    raise FileNotFoundError(
        "Ключ сервис-аккаунта не найден. "
        "На Render смонтируй Secret File в /etc/secrets/main_acc.json "
        "или задай GOOGLE_SERVICE_ACCOUNT_FILE."
    )

def parse_http_error(he: HttpError) -> Tuple[int, str]:
    code = getattr(he, "status_code", None) or getattr(getattr(he, "resp", None), "status", None) or 0
    msg = ""
    try:
        payload = json.loads(he.content.decode("utf-8"))
        msg = payload.get("error", {}).get("message") or payload.get("error_description") or ""
    except Exception:
        pass
    return int(code), msg

def drive_search(drive, q, page_size=100, page_token=None,
                 fields="files(id,name,mimeType,webViewLink,webContentLink,parents),nextPageToken"):
    resp = drive.files().list(
        q=q,
        corpora="allDrives",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields=fields,
        pageSize=page_size,
        pageToken=page_token
    ).execute()
    return resp.get("files", []), resp.get("nextPageToken")

def get_view_link(drive, file_id) -> Optional[str]:
    meta = drive.files().get(
        fileId=file_id,
        fields="webViewLink,webContentLink",
        supportsAllDrives=True
    ).execute()
    return meta.get("webViewLink") or meta.get("webContentLink")

# ========= Работа с папками/файлами клиента =========
def list_child_folders(drive, parent_id, page_size=200):
    q = (f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false")
    items, token = [], None
    while True:
        files, token = drive_search(drive, q, page_size=page_size, page_token=token, fields="files(id,name),nextPageToken")
        items.extend(files)
        if not token:
            break
    return items

def find_plan_file(drive, folder_id):
    target_exact = "Б. План Вашого звільнення.docx"
    q1 = f"'{folder_id}' in parents and name = '{target_exact}' and trashed=false and mimeType != 'application/vnd.google-apps.folder'"
    res, _ = drive_search(drive, q1, page_size=5, fields="files(id,name,mimeType),nextPageToken")
    if res:
        return res[0]
    q2 = f"'{folder_id}' in parents and name contains 'План Вашого звільнення' and trashed=false and mimeType != 'application/vnd.google-apps.folder'"
    res, _ = drive_search(drive, q2, page_size=20, fields="files(id,name,mimeType),nextPageToken")
    if res:
        pref = {
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": 3,
            "application/vnd.google-apps.document": 2
        }
        res.sort(key=lambda f: pref.get(f.get("mimeType",""), 1), reverse=True)
        return res[0]
    return None

# ======== Нормализация ФИО/телефона ========
APOSTROPHES = ["’", "`", "ʼ", "ʹ", "′", "＇", "ꞌ"]

def _unify_apostrophes(s: str) -> str:
    if not s: return ""
    for a in APOSTROPHES: s = s.replace(a, "'")
    return s

def normalize_spaces(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("\u00A0", " ")
    return re.sub(r"\s+", " ", s).strip()

def _smart_title(token: str) -> str:
    token = token.strip()
    if not token: return ""
    parts_by_dash = token.split("-")
    titled_parts = []
    for part in parts_by_dash:
        sub = part.split("'")
        sub = [(x[:1].upper() + x[1:].lower()) if x else x for x in sub]
        titled_parts.append("'".join(sub))
    return "-".join(titled_parts)

def normalize_fio_string(raw: str) -> str:
    s = normalize_spaces(_unify_apostrophes(raw or ""))
    s = re.sub(r"[,\.;]+", " ", s)
    s = normalize_spaces(s)
    tokens = [t for t in s.split(" ") if t]
    tokens = [_smart_title(t) for t in tokens]
    return " ".join(tokens)

def build_fio_from_contact(contact: dict) -> str:
    last = normalize_fio_string(contact.get("LAST_NAME") or "")
    first = normalize_fio_string(contact.get("NAME") or "")
    middle = normalize_fio_string(contact.get("SECOND_NAME") or "")
    parts = [p for p in [last, first, middle] if p]
    return " ".join(parts).strip()

def normalize_phone_e164_ua(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if not digits: return "+380"
    if digits.startswith("0"): digits = "380" + digits
    if digits.startswith("380"): body = digits[3:]
    else: body = digits.lstrip("380")
    body = body[-9:]
    return "+380" + body

def build_folder_title(fio: str, phone_e164: str) -> str:
    return f"{normalize_fio_string(fio)}, {normalize_phone_e164_ua(phone_e164)}"

def normalize_folder_title_for_compare(title: str) -> str:
    title = normalize_spaces(_unify_apostrophes(title or ""))
    m = re.match(r"^(.*?),(.*)$", title)
    if not m:
        return normalize_fio_string(title)
    left, right = m.group(1).strip(), m.group(2).strip()
    left = normalize_fio_string(left)
    digits = re.sub(r"\D", "", right)
    phone = normalize_phone_e164_ua(digits) if digits else normalize_phone_e164_ua(right)
    return f"{left}, {phone}"

# ======== Поиск папки (exact -> fuzzy -> по телефону) ========
def _extract_e164_from_title(title: str) -> Optional[str]:
    digits = re.sub(r"\D", "", title or "")
    if len(digits) < 9:
        return None
    m = re.findall(r"\d{9,12}", digits)
    if not m:
        return None
    return normalize_phone_e164_ua(m[-1])

def find_folder_by_exact_name_under(drive, parent_id: str, exact_name: str) -> Optional[dict]:
    q = (f"'{parent_id}' in parents and name = '{exact_name}' "
         f"and mimeType='application/vnd.google-apps.folder' and trashed=false")
    res, _ = drive_search(drive, q, page_size=1, fields="files(id,name),nextPageToken")
    return res[0] if res else None

def find_folder_by_fuzzy(drive, parent_id: str, fio_norm: str, phone_last9: str) -> Optional[dict]:
    q = (f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' "
         f"and name contains '{fio_norm.split(',')[0]}' and trashed=false")
    items, token = [], None
    while True:
        batch, token = drive_search(drive, q, page_size=100, page_token=token, fields="files(id,name),nextPageToken")
        items.extend(batch)
        if not token:
            break
    if phone_last9:
        items = [f for f in items if phone_last9 in re.sub(r"\D", "", f["name"])]
    if items:
        items.sort(key=lambda f: len(f["name"]), reverse=True)
        return items[0]
    return None

def find_folder_by_phone(drive, parent_id: str, phone_e164: str, expected_fio: Optional[str] = None) -> Optional[dict]:
    e164 = normalize_phone_e164_ua(phone_e164)
    digits = re.sub(r"\D", "", e164)          # 380XXXXXXXXX
    last9  = digits[-9:]
    op2, mid3, last4 = last9[:2], last9[2:5], last9[5:9]
    patterns = [
        f"{op2} {mid3} {last4}",
        f"{mid3} {last4}",
        last4,
        last9,
        ("+380 " + op2 + " " + mid3 + " " + last4),
        ("+380" + op2 + mid3 + last4),
    ]
    conds = " or ".join([f"name contains '{p}'" for p in patterns if "'" not in p])
    q = (f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' "
         f"and trashed=false and ({conds})")
    seen: Dict[str, dict] = {}
    items, token = [], None
    while True:
        batch, token = drive_search(drive, q, page_size=100, page_token=token, fields="files(id,name),nextPageToken")
        for f in batch:
            seen.setdefault(f["id"], f)
        if not token:
            break
    matched = []
    for f in seen.values():
        cand = _extract_e164_from_title(f["name"])
        if cand and normalize_phone_e164_ua(cand) == e164:
            matched.append(f)
    if not matched:
        return None
    if len(matched) == 1 or not expected_fio:
        matched.sort(key=lambda x: len(x["name"]), reverse=True)
        return matched[0]
    def tokens(s: str) -> set:
        s = normalize_spaces(s.split(",")[0])
        return set(t for t in s.split() if t)
    exp = tokens(expected_fio)
    matched.sort(key=lambda x: (len(tokens(x["name"]) & exp), len(x["name"])), reverse=True)
    return matched[0]

def find_client_folder_strict(drive, root_folder_id, expected_title: str, phone_e164: str):
    # 1) exact
    exact = find_folder_by_exact_name_under(drive, root_folder_id, expected_title)
    if exact:
        return exact
    # 2) fuzzy по ФИО + последние 9 цифр
    fio_only = normalize_folder_title_for_compare(expected_title)
    last9 = re.sub(r"\D", "", phone_e164)[-9:]
    fuzzy = find_folder_by_fuzzy(drive, root_folder_id, fio_only, last9)
    if fuzzy:
        return fuzzy
    # 3) по телефону
    return find_folder_by_phone(drive, root_folder_id, phone_e164, expected_fio=expected_title)

# ======== Bitrix ========
def _b24_base() -> str:
    if BITRIX_WEBHOOK_BASE:
        return BITRIX_WEBHOOK_BASE.rstrip("/") + "/"
    m = re.match(r"^(https://[^/]+/rest/\d+/[^/]+/)", BITRIX_CONTACT_URL)
    if not m:
        raise RuntimeError("Не удалось определить базовый URL Bitrix")
    return m.group(1)

def _b24_domain() -> str:
    base = _b24_base()
    m = re.match(r"^(https://[^/]+)/", base)
    if not m:
        raise RuntimeError("Не удалось определить домен Bitrix")
    return m.group(1)

def b24_post(method: str, payload: dict = None):
    url = f"{_b24_base()}{method}.json"
    r = http_post(url, json=payload or {})
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"B24 error {data.get('error')}: {data.get('error_description')}")
    return data.get("result")

def find_contact_by_phone(phone: str):
    norm = normalize_phone_e164_ua(phone)
    if BITRIX_CONTACT_URL:
        r = http_get(
            BITRIX_CONTACT_URL,
            params={"filter[PHONE]": norm, "select[]": ["ID","NAME","LAST_NAME","SECOND_NAME","PHONE"]},
        )
    else:
        url = _b24_base() + "crm.contact.list.json"
        r = http_get(url, params={"filter[PHONE]": norm, "select[]": ["ID","NAME","LAST_NAME","SECOND_NAME","PHONE"]})
    r.raise_for_status()
    data = r.json()
    result = data.get("result", [])
    if not result:
        return None
    want = re.sub(r"\D", "", norm)
    for c in result:
        for ph in c.get("PHONE", []):
            if re.sub(r"\D", "", ph.get("VALUE", "")) == want:
                return c
    return None

def try_get_user_name(user_id: int) -> str:
    try:
        users = b24_post("user.get", {"ID": user_id})
        if users and isinstance(users, list):
            u = users[0]
            parts = [u.get("NAME") or "", u.get("LAST_NAME") or "", u.get("SECOND_NAME") or ""]
            name = " ".join([p for p in parts if p]).strip()
            return name or f"ID {user_id}"
    except Exception:
        return f"ID {user_id} (немає прав)"
    return f"ID {user_id}"

def resolve_consultant(value):
    if value is None or value == "":
        return "—"
    if isinstance(value, (list, tuple, set)):
        names = []
        for v in value:
            s = str(v).strip()
            names.append(try_get_user_name(int(s)) if s.isdigit() else s)
        return ", ".join(names) if names else "—"
    s = str(value).strip()
    return try_get_user_name(int(s)) if s.isdigit() else s

def get_last_deal_for_contact(contact_id: int, category_id: int):
    deals = b24_post("crm.deal.list", {
        "filter": {"CONTACT_ID": contact_id, "CATEGORY_ID": category_id},
        "select": ["ID","TITLE","STAGE_ID","ASSIGNED_BY_ID","DATE_CREATE", CONSULTANT_FIELD, "CATEGORY_ID", "UF_CRM_62F6731E2FFAF", "UF_CRM_1660157603"],
        "order":  {"DATE_CREATE": "DESC"}
    })
    return deals[0] if deals else None

def get_stage_map_for_category(category_id: int):
    items = b24_post("crm.dealcategory.stage.list", {"id": category_id})
    return {it["STATUS_ID"]: it["NAME"] for it in items}

# ======== История стадий / длительность ========
_STAGE_MAP_CACHE: Dict[int, Dict[str, str]] = {}

def _stage_name_by_sid(stage_id: str) -> str:
    if not stage_id:
        return "—"
    m = re.match(r"^C(\d+):", stage_id)
    if not m:
        return stage_id
    cat = int(m.group(1))
    if cat not in _STAGE_MAP_CACHE:
        _STAGE_MAP_CACHE[cat] = get_stage_map_for_category(cat)
    return _STAGE_MAP_CACHE[cat].get(stage_id, stage_id)

def _parse_iso(ts: str) -> datetime:
    ts = (ts or "").replace("Z", "+00:00")
    return datetime.fromisoformat(ts)

def _fmt_tdelta(td: timedelta) -> str:
    total = int(max(td.total_seconds(), 0))
    d, rem = divmod(total, 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    parts = []
    if d: parts.append(f"{d} д")
    if h: parts.append(f"{h} год")
    if m or not parts: parts.append(f"{m} хв")
    return " ".join(parts)

def get_deal_stage_history(deal_id: int, asc: bool = True, limit: int = 300) -> List[dict]:
    payload = {
        "entityTypeId": 2,
        "filter": {"OWNER_ID": int(deal_id)},
        "order": {"CREATED_TIME": "ASC" if asc else "DESC"},
        "select": ["ID","OWNER_ID","STAGE_ID","CREATED_TIME","CATEGORY_ID"],
        "start": 0
    }
    res = b24_post("crm.stagehistory.list", payload)
    items = res.get("items", res) if isinstance(res, dict) else (res or [])
    items = items[:limit]
    items.sort(key=lambda r: r.get("CREATED_TIME", ""))

    out, prev_sid = [], None
    for r in items:
        sid = r.get("STAGE_ID")
        if sid == prev_sid:
            continue
        out.append(r); prev_sid = sid
    return out

def compute_stage_segments(rows: List[dict]) -> List[dict]:
    segs = []
    for i, r in enumerate(rows):
        start = _parse_iso(r.get("CREATED_TIME"))
        end = _parse_iso(rows[i + 1]["CREATED_TIME"]) if i + 1 < len(rows) else datetime.now(start.tzinfo or timezone.utc)
        segs.append({
            "stage_id": r.get("STAGE_ID"),
            "start": start,
            "end": end,
            "duration": end - start,
        })
        # end defaults to "now" for the last segment
    return segs

# ======== /check ========
CHECK_RX = re.compile(r"^\s*/?check\s+(.+)$", re.IGNORECASE)

def handle_check(update: Update, ctx: CallbackContext, raw_phone: str):
    phone = normalize_phone_e164_ua(raw_phone)
    if len(re.sub(r"\D", "", phone)) < 12:
        update.message.reply_text("Будь ласка, надішліть номер у форматі: /check +380XXXXXXXXX")
        return

    contact = find_contact_by_phone(phone)
    if not contact:
        update.message.reply_text("❌ Клієнта з таким номером у CRM не знайдено.")
        return

    client_fio = build_fio_from_contact(contact)
    contact_id = int(contact["ID"])

    deal = get_last_deal_for_contact(contact_id, CATEGORY_ID)
    if not deal:
        update.message.reply_text(f"ℹ️ У клієнта немає угоди у воронці №{CATEGORY_ID}.")
        return

    deal_id = int(deal["ID"])
    stage_map = get_stage_map_for_category(CATEGORY_ID)
    stage_name = stage_map.get(deal.get("STAGE_ID"), deal.get("STAGE_ID") or "—")
    resp_id = int(deal.get("ASSIGNED_BY_ID") or 0)
    resp_name = try_get_user_name(resp_id) if resp_id else "—"

    consultant_raw = deal.get(CONSULTANT_FIELD)
    consultant_name = resolve_consultant(consultant_raw)

    deal_link = f"{_b24_domain()}/crm/deal/details/{deal_id}/"
    
    debt = deal.get("UF_CRM_62F6731E2FFAF") or "—"  # сумма из сделки
    court = deal.get("UF_CRM_1660157603") or "—"  # court из контакта

    # ==== Google Drive ====
    doc_line = "📎 <b>Документи:</b> —"
    try:
        drive = build_drive()
        expected_folder_title = build_folder_title(client_fio, phone)

        folder = find_client_folder_strict(drive, DRIVE_ROOT_FOLDER_ID, expected_folder_title, phone)
        if folder:
            plan = find_plan_file(drive, folder["id"])
            if plan:
                view = get_view_link(drive, plan["id"])
                doc_line = f'📎 <b>Документи:</b> <a href="{view}">Б. План Вашого звільнення</a>'
            else:
                doc_line = "📎 <b>Документи:</b> План не знайдено у папці клієнта"
        else:
            doc_line = "📎 <b>Документи:</b> Папку клієнта не знайдено"
    except HttpError as he:
        code, reason = parse_http_error(he)
        doc_line = f"📎 <b>Документи:</b> Помилка доступу до Drive ({code} {reason})"
    except Exception as e:
        log.exception("Drive error")
        doc_line = f"📎 <b>Документи:</b> Помилка: {e}"

    # ==== История стадий / длительность текущей ====
    stage_extra = ""
    history_block = ""
    try:
        hist = get_deal_stage_history(deal_id, asc=True)
        if hist:
            segs = compute_stage_segments(hist)
            cur = segs[-1]
            cur_dur = _fmt_tdelta(cur["duration"])
            stage_extra = f" ({cur_dur})"

            lines = []
            for i, s in enumerate(segs):
                start_s = s["start"].strftime('%Y-%m-%d %H:%M')
                dur_s = _fmt_tdelta(s["duration"])
                name = _stage_name_by_sid(s["stage_id"])
                tail = " (поточна)" if i == len(segs) - 1 else f" (до {s['end'].strftime('%Y-%m-%d %H:%M')})"
                lines.append(f"• {start_s} → {name} — {dur_s}{tail}")
            history_block = "🧭 <b>Історія стадій:</b>\n" + "\n".join(lines)
    except Exception as e:
        log.warning("stage history error: %s", e)

    text = (
        f"📄 <b>Клієнт:</b> {client_fio}\n"
        f"📊 <b>Угода:</b> №{deal_id} — {deal.get('TITLE','')}\n"
        f"🔗 <b>Посилання:</b> <a href=\"{deal_link}\">відкрити угоду</a>\n"
        f"📌 <b>Стадія:</b> {stage_name}{stage_extra}\n"
        f"👨‍💼 <b>Відповідальний юрист:</b> {resp_name}\n"
        f"🧑‍💼 <b>Менеджер з продажу:</b> {consultant_name}\n"
        f"🏠 <b>Суд:</b> {court}\n"
        f"💰 <b>Загальна сума боргу:</b> {debt}\n"
        f"{doc_line}"
    )
    if history_block:
        text += "\n" + history_block

    update.message.reply_text(text, parse_mode="HTML", disable_web_page_preview=True)

# ======== Роутинг ========
def on_text(update: Update, ctx: CallbackContext):
    msg = update.message.text or ""
    m = CHECK_RX.match(msg)
    if m:
        return handle_check(update, ctx, m.group(1).strip())

def on_check_cmd(update: Update, ctx: CallbackContext):
    raw = " ".join(ctx.args) if ctx.args else ""
    if not raw:
        update.message.reply_text("Будь ласка, надішліть номер у форматі: /check +380XXXXXXXXX")
        return
    handle_check(update, ctx, raw)

# ======== Запуск ========
def main():
    _assert_required_env()

    # self-test Drive (по желанию: сразу поймёшь, что папка не расшарена)
    try:
        drive = build_drive()
        info = drive.files().get(fileId=DRIVE_ROOT_FOLDER_ID, fields="id,name", supportsAllDrives=True).execute()
        log.info("[drive] root OK: %s (%s)", info.get("name"), info.get("id"))
    except Exception as e:
        log.error("Drive self-test failed: %s", e)
        raise

    updater = Updater(
        BOT_TOKEN,
        use_context=True,
        request_kwargs={"read_timeout": 30, "connect_timeout": 10},
    )

    # Чтобы не было 409 на Render, отключаем возможный вебхук:
    try:
        updater.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    dp = updater.dispatcher
    dp.add_handler(CommandHandler("check", on_check_cmd, pass_args=True))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, on_text))

    try:
        updater.start_polling(timeout=30, drop_pending_updates=True)
        updater.idle()
    except TgConflict:
        log.error("Conflict 409: другой процесс уже выполняет getUpdates. Останови лишний инстанс.")
        raise

if __name__ == "__main__":
    main()
