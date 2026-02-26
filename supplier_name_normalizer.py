import csv
import json
import re
import time
import unicodedata
import logging
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import requests

# ========================================
# LOGGING
# ========================================
logger = logging.getLogger("supplier_normalizer")

# ========================================
# CONSTANTS
# ========================================

# --- Legal Suffix Removal (Cleanco-style) ---
LEGAL_SUFFIXES = {
    "inc", "incorporated", "corp", "corporation", "co", "company", "ltd", "limited",
    "llc", "llp", "plc", "gmbh", "ag", "kg", "ug", "sa", "sar1", "sas", "sr1",
    "eur1", "bv", "nv", "pty", "pvt", "ab", "oy", "as", "aps", "sp", "sro", "doo",
    "kft", "zrt", "nyrt", "mbh", "ev", "eg", "ohg", "kgaa", "se", "scr1", "spr1",
    "cvba", "vof", "bvba", "snc", "sca", "scs", "spol", "ood", "ad", "ead", "eood",
    "jsc", "ojsc", "cjsc", "pjsc", "zao", "ooo", "oao", "pao", "tov", "pp",
    "bhd", "sdn", "pt", "tbk", "cv", "firma", "gruppen", "group", "holding",
    "holdings", "international", "intl", "enterprises", "industries", "services",
    "solutions", "technologies", "technology", "tech", "systems", "global",
    "worldwide", "associates", "partners", "consulting", "consultants",
    "de", "del", "la", "el", "les", "los", "das", "der", "die", "het", "van", "von",
}

# --- Prefix words to strip from the BEGINNING of names ---
STRIP_PREFIX_WORDS = {"the", "a", "an", "mr", "mrs", "ms", "dr", "prof", "st", "saint"}

# --- Comprehensive first name dictionary (~1200 names across cultures) ---
FIRST_NAMES = {
    # English - Male
    "james", "john", "robert", "michael", "william", "david", "richard", "joseph", "thomas", "charles",
    "christopher", "daniel", "matthew", "anthony", "mark", "donald", "steven", "paul", "andrew", "joshua",
    "kenneth", "kevin", "brian", "george", "timothy", "ronald", "edward", "jason", "jeffrey", "ryan",
    "jacob", "gary", "nicholas", "eric", "jonathan", "stephen", "larry", "justin", "scott", "brandon",
    "benjamin", "samuel", "raymond", "gregory", "frank", "alexander", "patrick", "jack", "dennis", "jerry",
    "tyler", "aaron", "jose", "adam", "nathan", "henry", "peter", "zachary", "douglas", "harold",
    "kyle", "noah", "gerald", "carl", "keith", "roger", "jeremy", "terry", "sean", "austin",
    "arthur", "lawrence", "jesse", "dylan", "bryan", "joe", "jordan", "billy", "bruce", "albert",
    "willie", "gabriel", "logan", "ralph", "eugene", "russell", "bobby", "mason", "philip", "louis",
    "wayne", "randy", "vincent", "liam", "ethan", "aiden", "owen", "luke", "connor", "ian",
    # English - Female
    "mary", "patricia", "jennifer", "linda", "barbara", "elizabeth", "susan", "jessica", "sarah", "karen",
    "lisa", "nancy", "betty", "margaret", "sandra", "ashley", "dorothy", "kimberly", "emily", "donna",
    "michelle", "carol", "amanda", "melissa", "deborah", "stephanie", "rebecca", "sharon", "laura", "cynthia",
    "kathleen", "amy", "angela", "shirley", "anna", "brenda", "pamela", "emma", "nicole", "helen",
    "samantha", "katherine", "christine", "debra", "rachel", "carolyn", "janet", "catherine", "maria", "heather",
    "diane", "ruth", "julie", "olivia", "joyce", "virginia", "victoria", "kelly", "lauren", "christina",
    "joan", "evelyn", "judith", "megan", "andrea", "cheryl", "hannah", "jacqueline", "martha", "gloria",
    "teresa", "ann", "sara", "madison", "frances", "kathryn", "janice", "jean", "abigail", "alice",
    "julia", "judy", "sophia", "grace", "denise", "amber", "doris", "marilyn", "danielle", "beverly",
    "isabella", "theresa", "diana", "natalie", "brittany", "charlotte", "marie", "kayla", "alexis", "lori",
    # Indian
    "aarav", "aditya", "akash", "akshay", "akshaye", "amit", "amitabh", "anil", "ankit", "anurag",
    "arjun", "arun", "ashish", "bharat", "chandra", "deepak", "dev", "dhruv", "dinesh", "ganesh",
    "gaurav", "gopal", "hari", "harsh", "hemant", "ishaan", "jagdish", "jay", "karan", "kartik",
    "krishna", "kumar", "lalit", "mahesh", "manoj", "mohit", "mukesh", "naman", "naresh", "nikhil",
    "nitin", "pankaj", "pranav", "prashant", "rahul", "raj", "rajesh", "rajan", "ravi",
    "rohit", "sachin", "sanjay", "sanjeev", "satish", "shiv", "shyam", "siddharth", "sunil", "suresh",
    "tushar", "varun", "vijay", "vikram", "vinay", "vinod", "vipin", "vishal", "vivek", "yash",
    "aarti", "ananya", "anjali", "anita", "asha", "bhavna", "chitra", "deepa", "divya", "durga",
    "gayatri", "geeta", "isha", "jaya", "jyoti", "kajal", "kavita", "komal", "lata", "madhuri",
    "mamta", "meena", "megha", "mira", "nandini", "neha", "nisha", "nita", "padma", "pallavi",
    "pooja", "priya", "radha", "rashmi", "rekha", "renu", "rina", "ritu", "roshni", "sakshi",
    "sangeeta", "sarita", "seema", "shanti", "shilpa", "shreya", "sita", "smita", "sneha", "sonali",
    "sonia", "sudha", "sunita", "sushma", "swati", "tanvi", "uma", "usha", "vandana", "vidya",
    # Chinese (romanized)
    "wei", "fang", "lei", "jing", "ling", "yan", "hui", "xin",
    "ming", "hong", "ping", "chun", "dong", "feng", "hai", "jie", "jun", "kai",
    "bin", "bo", "chang", "cheng", "gang", "guo", "hao", "hua", "jian", "liang",
    "lin", "long", "peng", "qiang", "rong", "shan", "tao", "wen", "xiang", "yi",
    "yong", "yuan", "yun", "zhi", "zhong",
    # Hispanic
    "alejandro", "alfredo", "andres", "angel", "antonio", "arturo", "carlos", "cesar", "cristian", "diego",
    "eduardo", "enrique", "ernesto", "felipe", "fernando", "francisco", "guillermo", "gustavo", "hector", "hugo",
    "ignacio", "ivan", "javier", "jesus", "joaquin", "jorge", "juan", "julio", "leonardo", "luis",
    "manuel", "marco", "marcos", "mario", "martin", "miguel", "nicolas", "oscar", "pablo", "pedro",
    "rafael", "ramon", "raul", "ricardo", "ruben", "rodrigo", "salvador", "sergio", "victor",
    "adriana", "alejandra", "alicia", "ana", "beatriz", "camila", "carmen", "catalina", "claudia", "daniela",
    "elena", "fernanda", "gabriela", "guadalupe", "isabel", "jimena", "lucia", "luisa", "margarita",
    "monica", "natalia", "paola", "patricia", "rosa", "silvia", "sofia", "valentina", "valeria", "veronica",
    # Arabic
    "abdul", "abdullah", "ahmad", "ahmed", "ali", "amir", "bilal", "farid", "hamza", "hasan",
    "hassan", "hussein", "ibrahim", "imran", "ismail", "jamal", "kareem", "khalid", "mahmoud", "mansour",
    "mohammed", "mohamad", "muhammad", "mustafa", "nabil", "nader", "nasir", "omar", "rashid",
    "saeed", "said", "saleh", "sami", "tariq", "walid", "youssef", "zaid",
    "aisha", "amina", "ayesha", "fatima", "hana", "khadija", "layla", "leila", "mariam",
    "maryam", "nadia", "noura", "rania", "reem", "salma", "yasmin", "zahra",
    # European
    "anders", "bjorn", "erik", "gustav", "hans", "henrik", "ingrid", "johan", "karl", "lars",
    "magnus", "nils", "olaf", "sven", "axel", "astrid", "brigitte", "elsa",
    "franz", "fritz", "gerhard", "gunther", "heinrich", "helmut", "jurgen", "klaus", "ludwig",
    "manfred", "otto", "rolf", "siegfried", "ulrich", "werner", "wolfgang", "claude", "francois", "jacques",
    "jean", "marcel", "philippe", "pierre", "rene", "yves", "andre", "antoine",
    "benoit", "christophe", "dominique", "etienne", "florian", "guillaume",
    "laurent", "mathieu", "olivier", "pascal", "raphael", "sebastien", "thierry",
    "giovanni", "giuseppe", "luca", "matteo", "paolo", "roberto", "angelo", "bruno",
    "carlo", "dario", "fabio", "giorgio", "lorenzo", "massimo", "nicola", "pietro", "stefano",
    # Korean/Japanese romanized
    "hyun", "jin", "sang", "seung", "soo", "sung", "won", "young",
    "akira", "haruki", "hiroshi", "kenji", "makoto", "naoki", "satoshi", "takeshi",
    "yuki", "haruka", "keiko", "megumi", "sakura", "yumi",
    # Common short names
    "bob", "rob", "ted", "ed", "al", "ben", "dan", "don", "jim", "jon", "sam", "tim", "tom", "pat", "sue", "lee",
    "max", "ray", "roy", "rex", "bud", "hal", "ned", "walt", "hank", "chuck", "rick", "nick", "mike", "dave",
    "steve", "chris", "matt", "jeff", "greg", "craig", "brad", "chad", "derek", "troy", "wade", "dean", "dale",
}

# --- Titles that strongly indicate a person ---
PERSON_TITLES = {
    "mr", "mrs", "ms", "miss", "dr", "prof", "sir", "dame", "rev", "reverend",
    "capt", "captain", "sgt", "sergeant", "lt", "lieutenant", "col", "colonel",
    "gen", "general", "hon", "honorable", "judge", "justice", "rabbi", "imam", "pastor",
    "brother", "sister", "father", "mother", "jr", "sr", "ii", "iii", "iv",
}

# --- Corporate keywords that strongly indicate an organization ---
CORP_KEYWORDS = {
    "group", "associates", "partners", "consulting", "consultants", "services", "solutions",
    "technologies", "technology", "tech", "systems", "global", "worldwide", "enterprises",
    "industries", "international", "intl", "holdings", "holding", "management", "capital",
    "financial", "insurance", "logistics", "supply", "manufacturing", "mfg", "construction",
    "engineering", "design", "media", "communications", "network", "networks", "pharma",
    "pharmaceutical", "medical", "healthcare", "health", "clinic", "hospital", "university",
    "college", "institute", "foundation", "society", "association", "federation", "council",
    "bureau", "agency", "authority", "department", "division",
    "bank", "trust", "fund", "realty", "property", "properties", "development",
    "foods", "food", "beverage", "restaurant", "hotel", "resort",
    "airlines", "airways", "motors", "auto", "automotive", "electric", "energy", "power",
    "petroleum", "oil", "gas", "mining", "metals", "steel", "chemical", "chemicals",
    "textiles", "apparel", "furniture", "equipment", "tools", "hardware",
    "software", "digital", "data", "cloud", "cyber", "security", "defense", "defence",
    "transport", "transportation", "freight", "shipping", "marine", "aviation",
    "telecom", "telecommunications", "wireless", "publishing",
    "studio", "studios", "entertainment", "gaming",
    "retail", "wholesale", "distribution", "distributors", "imports", "exports", "trading",
    "ventures", "innovations", "labs", "laboratory", "laboratories", "research",
    "cooperative", "coop", "union", "works", "factory", "plant", "mill",
    "store", "stores", "shop", "shops", "market", "markets", "mart", "plaza", "center", "centre",
    "depot", "warehouse", "exchange",
}

# --- Strong legal suffixes (subset used for entity classification) ---
STRONG_LEGAL_SUFFIXES = {
    "inc", "incorporated", "corp", "corporation", "llc", "llp", "ltd",
    "limited", "plc", "gmbh", "ag", "sa", "sar1", "sas", "sr1", "bv", "nv", "pty", "pvt",
}

# --- Person title suffixes ---
PERSON_TITLE_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "esq", "md", "phd", "dds", "cpa", "rn", "pe"}

# --- Common first words that create mega-groups if used alone as keys ---
COMMON_FIRST_WORDS = {
    "american", "national", "united", "general", "first", "new", "north", "south",
    "east", "west", "central", "pacific", "atlantic", "great", "royal", "standard",
    "advanced", "applied", "best", "blue", "city", "classic", "complete", "creative",
    "custom", "digital", "direct", "elite", "express", "five", "four", "golden",
    "green", "high", "home", "ideal", "key", "all", "star", "sun", "top", "total",
    "tri", "true", "twin", "us", "usa", "white", "world", "pro", "premier", "prime",
}


# ========================================
# DATA CLASSES
# ========================================

@dataclass
class NormalizationResult:
    """Single supplier name normalization result."""
    original: str
    normalized: str
    individual: bool
    cluster: str
    confidence: str      # high | medium | low | auto | error
    method: str          # singleton | llm-cluster | llm-singleton | llm-missed | fallback
    count: int           # number of raw rows this cleaned name represents


@dataclass
class EntityClassification:
    """Person vs Organization classification result."""
    type: str            # individual | organization | unknown
    confidence: str      # high | medium | low
    reason: str          # rule that triggered the classification


@dataclass
class UniqueNameEntry:
    """Internal tracking for a unique cleaned name."""
    cleaned: str
    originals: dict      # raw_name -> count
    indices: list
    entity_type: str = "unknown"
    is_individual: bool = False

    @property
    def best_original(self) -> str:
        """Most frequently occurring raw form."""
        return max(self.originals, key=self.originals.get)

    @property
    def total_count(self) -> int:
        return len(self.indices)


# ========================================
# CORE FUNCTIONS
# ========================================

def clean_name(raw: str) -> str:
    """
    Deterministic name cleaning:
    - Unicode normalization (NFKD, strip combining marks)
    - Lowercase
    - Replace & with 'and'
    - Remove DBA / AKA / FKA / C/O / ATTN markers
    - Strip non-alphanumeric (keep spaces and hyphens)
    - Remove standalone short numbers (1-3 digits)
    - Remove legal suffixes from end
    - Remove filler prefix words (the, a, an, etc.)
    """
    if not raw:
        return ""
    s = str(raw).strip()
    # Skip if looks like a number, date, or blank
    if re.match(r"^\d+$", s) or len(s) < 2:
        return ""
    # Normalize unicode
    s = unicodedata.normalize("NFKD", s)
    s = re.sub(r"[\u0300-\u036f]", "", s)
    # Lowercase
    s = s.lower()
    # Replace & with and
    s = s.replace("&", " and ")
    # Normalize common patterns
    s = re.sub(r"\bdba\b", "", s)           # "doing business as"
    s = re.sub(r"\bd/b/a\b", "", s)
    s = re.sub(r"\baka\b", "", s)
    s = re.sub(r"\ba/k/a\b", "", s)
    s = re.sub(r"\bfka\b", "", s)           # "formerly known as"
    s = re.sub(r"\bc/o\b", "", s)           # "care of"
    s = re.sub(r"\battn\b", "", s)
    # Remove common punctuation but keep alphanumeric, spaces, hyphens
    s = re.sub(r"[^\w\s-]", " ", s)
    # Remove standalone single digits/short numbers (like account codes)
    s = re.sub(r"\b\d{1,3}\b", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # Remove legal suffixes from end
    tokens = s.split(" ")
    while len(tokens) > 1 and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    # Remove filler prefix words (the, a, an, etc.)
    while len(tokens) > 1 and tokens[0] in STRIP_PREFIX_WORDS:
        tokens.pop(0)
    return " ".join(tokens).strip()


def classify_entity(raw_name: str) -> EntityClassification:
    """
    Classify a supplier name as individual (person) or organization.
    
    Uses a layered rule-based approach (cheapest-first):
        1. Strong legal suffix → ORG (high)
        2. Title prefix → PERSON (high)
        3. Title suffix (Jr, Sr, MD, Esq) → PERSON (high)
        4. Corporate keywords → ORG (high)
        5. & or + between words → ORG (medium)
        6. Numbers in name → ORG (medium)
        7. "Last, First" comma pattern with known first name → PERSON (high)
        8. First-name dictionary + 2-3 word name → PERSON (medium)
        9. Single word → ORG (low)
        10. 4+ words → ORG (low)
    """
    if not raw_name:
        return EntityClassification(type="unknown", confidence="low", reason="empty")
    
    s = str(raw_name).strip()
    lower = re.sub(r"[^\w\s&]", " ", s.lower())
    lower = re.sub(r"\s+", " ", lower).strip()
    tokens = lower.split(" ")
    
    # RULE 1: Legal suffix → definitely ORG
    for t in tokens:
        if t in STRONG_LEGAL_SUFFIXES:
            return EntityClassification(type="organization", confidence="high", reason="legal_suffix")
    
    # RULE 2: Title prefix → definitely PERSON
    if tokens[0] in PERSON_TITLES:
        return EntityClassification(type="individual", confidence="high", reason="title_prefix")
    
    # RULE 3: Title suffix (Jr, Sr, II, III, MD, Esq)
    last_token = tokens[-1]
    if last_token in PERSON_TITLE_SUFFIXES:
        return EntityClassification(type="individual", confidence="high", reason="title_suffix")
    
    # RULE 4: Corporate keywords → ORG
    for t in tokens:
        if t in CORP_KEYWORDS:
            return EntityClassification(type="organization", confidence="high", reason="corp_keyword")
    
    # RULE 5: & or + between words → likely ORG
    if re.search(r"[&+]", s) and len(tokens) >= 3:
        return EntityClassification(type="organization", confidence="medium", reason="ampersand")
    
    # RULE 6: Numbers in name → likely ORG
    if re.search(r"\d", s):
        return EntityClassification(type="organization", confidence="medium", reason="has_numbers")
    
    # RULE 7: "Last, First" comma pattern → PERSON
    cleaned_for_comma = re.sub(r"[^\w\s,]", "", s)
    if re.match(r"^[a-z]+\s*,\s*[a-z]+", cleaned_for_comma, re.IGNORECASE):
        parts = [p.strip().lower().split(" ")[0] for p in s.split(",")]
        if len(parts) >= 2 and parts[1] in FIRST_NAMES:
            return EntityClassification(type="individual", confidence="high", reason="last_comma_first")
    
    # RULE 8: First-name dictionary + 2-3 word name → PERSON
    if 2 <= len(tokens) <= 3:
        first_name = tokens[0]
        if len(first_name) >= 2 and first_name in FIRST_NAMES:
            last_name = tokens[-1]
            if last_name not in CORP_KEYWORDS and last_name not in LEGAL_SUFFIXES:
                return EntityClassification(type="individual", confidence="medium", reason="firstname_match")
    
    # RULE 9: Single word
    if len(tokens) == 1:
        return EntityClassification(type="organization", confidence="low", reason="single_word")
    
    # RULE 10: 4+ words → more likely ORG
    if len(tokens) >= 4:
        return EntityClassification(type="organization", confidence="low", reason="long_name")
    
    # Can't determine
    return EntityClassification(type="unknown", confidence="low", reason="no_signal")


def get_group_key(cleaned: str) -> str:
    """
    Token-based grouping key (O(n) blocking replacement).

    Uses first token as key, but combines with second token when the first
    word is very common (to avoid mega-groups like "american" with 200+ members).
    """
    if not cleaned:
        return "_empty_"
    tokens = cleaned.split(" ")
    key = tokens[0]
    
    # For very short first tokens OR common first words, combine with second token
    if len(tokens) > 1 and (len(key) <= 2 or key in COMMON_FIRST_WORDS):
        key = f"{tokens[0]}_{tokens[1]}"
    
    # Extra: if still a very common combo and 3+ tokens, add third
    if len(tokens) > 2 and len(key) <= 5:
        key = f"{key}_{tokens[2]}"
    
    return key


def build_groups(names: list[UniqueNameEntry]) -> dict[str, list]:
    """
    Build token groups from unique name entries.
    Returns dict of group_key -> list of {original, cleaned, count, indices}.
    """
    groups: dict[str, list] = defaultdict(list)
    for entry in names:
        key = get_group_key(entry.cleaned)
        groups[key].append({
            "original": entry.best_original,
            "cleaned": entry.cleaned,
            "count": entry.total_count,
            "indices": entry.indices,
        })
    return dict(groups)


# ========================================
# GEMINI API
# ========================================

def safe_parse_json(text: str) -> dict:
    """
    Robust JSON parser with 4 fallback strategies to handle
    Gemini truncated/malformed responses.
    """
    s = text.strip()
    # Strip markdown fences
    s = re.sub(r"^```json\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^```\s*", "", s)
    s = re.sub(r"```\s*$", "", s)
    s = s.strip()
    
    # Attempt 1: direct parse
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    
    # Attempt 2: regex extract complete cluster objects
    try:
        cluster_pattern = r'\{"canonical"\s*:\s*"[^"]*"\s*,\s*"members"\s*:\s*\[[^\]]*\]\s*,\s*"confidence"\s*:\s*"[^"]*"\s*\}'
        matches = re.findall(cluster_pattern, s)
        if matches:
            fixed = '{"clusters":[' + ",".join(matches) + "]}"
            return json.loads(fixed)
    except json.JSONDecodeError:
        pass
    
    # Attempt 3: fix common JSON issues
    try:
        fixed = s
        fixed = re.sub(r',\s*(\]|\})', r"\1", fixed)  # trailing commas
        fixed = fixed.replace("'", '"')                # single quotes
        
        open_braces = fixed.count("{")
        close_braces = fixed.count("}")
        open_brackets = fixed.count("[")
        close_brackets = fixed.count("]")
        fixed += "]" * max(0, open_brackets - close_brackets)
        fixed += "}" * max(0, open_braces - close_braces)
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass
    
    # Attempt 4: extract any JSON object from the text
    try:
        match = re.search(r"\{[\s\S]*\}", s)
        if match:
            fixed = re.sub(r',\s*(\]|\})', r"\1", match.group(0))
            return json.loads(fixed)
    except json.JSONDecodeError:
        pass
    
    raise ValueError("Could not parse Gemini response as JSON")


def call_gemini(
    names: list[str],
    api_key: str,
    model: str = "gemini-2.5-flash",
) -> dict:
    """
    Send a batch of supplier names to Gemini for clustering.
    
    Returns dict with 'clusters' key containing list of:
    {"canonical": str, "members": [int], "confidence": str}
    """
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}"
        f":generateContent?key={api_key}"
    )
    
    # Escape any quotes in names to prevent JSON issues
    safe_names = [re.sub(r'"', " ", n).strip() for n in names]
    safe_names = [re.sub(r"\s+", " ", n) for n in safe_names]
    
    indexed_list = "\n".join(f"{i}: {n}" for i, n in enumerate(safe_names))
    
    prompt = f"""You are an expert at supplier/company name normalization for procurement spend data.

Given this list of company names, identify which names refer to the SAME real-world entity and group them.

RULES:
- Spelling variations, abbreviations, different legal suffixes of same company = SAME cluster
- Genuinely different companies = DIFFERENT clusters
- Pick the most complete/official name as canonical
- Every index must appear in exactly one cluster

NAMES:
{indexed_list}

Return JSON:
{{"clusters":[{{"canonical":"Name","members":[0,2],"confidence":"high"}}]}}

confidence: high/medium/low. Singletons get their own cluster."""
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.05,
            "maxOutputTokens": 16384,
            "responseMimeType": "application/json",
        },
    }
    
    response = requests.post(url, json=payload, timeout=120)
    
    if not response.ok:
        try:
            err = response.json()
            msg = err.get("error", {}).get("message", response.reason)
        except Exception:
            msg = response.reason
        raise RuntimeError(f"Gemini API error {response.status_code}: {msg}")
    
    data = response.json()
    text = (
        data.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
    )
    if not text:
        raise RuntimeError("Empty response from Gemini")
    
    return safe_parse_json(text)


# ========================================
# MAIN NORMALIZER CLASS
# ========================================

class SupplierNormalizer:
    """
    LLM-first supplier name normalization engine.

    Pipeline:
    1. Deterministic preprocessing (clean, deduplicate)  - O(n)
    1.5. Entity classification (Person vs Organization)  - O(n)
    2. Token-based grouping (O(n) blocking replacement)  - O(n)
    3. Gemini LLM clustering (semantic entity resolution) - API calls
    4. Canonical name selection + export
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gemini-2.5-flash",
        batch_size: int = 50,
        min_group_size: int = 2,
        max_retries: int = 3,
        delay_between_calls: float = 0.5,
        progress_callback: Optional[callable] = None,
    ):
        self.api_key = api_key
        self.model = model
        self.batch_size = batch_size
        self.min_group_size = min_group_size
        self.max_retries = max_retries
        self.delay_between_calls = delay_between_calls
        self.progress_callback = progress_callback

    def _log(self, msg: str, level: str = "info"):
        """Log a message and optionally call progress callback."""
        getattr(logger, level, logger.info)(msg)
        if self.progress_callback:
            self.progress_callback(msg, level)

    # ----- Stage 1: Extract & Clean -----

    def _extract_and_clean(self, raw_names: list[str]) -> dict[str, UniqueNameEntry]:
        """
        Clean raw names, deduplicate, return map of cleaned -> UniqueNameEntry.
        """
        self._log(f"Stage 1: Extracting and cleaning {len(raw_names):,} supplier names...")

        unique_map: dict[str, UniqueNameEntry] = {}

        for i, raw in enumerate(raw_names):
            raw = str(raw).strip() if raw else ""
            if not raw:
                continue
            cleaned = clean_name(raw)
            if not cleaned:
                continue

            if cleaned not in unique_map:
                unique_map[cleaned] = UniqueNameEntry(
                    cleaned=cleaned, originals={}, indices=[]
                )
            entry = unique_map[cleaned]
            entry.originals[raw] = entry.originals.get(raw, 0) + 1
            entry.indices.append(i)

        self._log(
            f"Extracted {len(raw_names):,} rows -> {len(unique_map):,} unique cleaned names",
        )
        return unique_map

    # ----- Stage 1.5: Entity Classification -----

    def _classify_entities(self, unique_map: dict[str, UniqueNameEntry]) -> int:
        """
        Classify each unique name as individual or organization.
        Returns count of individuals detected.
        """
        self._log("Classifying entities (Person vs Organization)...")
        individual_count = 0

        for cleaned, entry in unique_map.items():
            classification = classify_entity(entry.best_original)
            entry.entity_type = classification.type
            entry.is_individual = classification.type == "individual"
            if entry.is_individual:
                individual_count += 1

        self._log(
            f"Entity classification complete: {individual_count:,} individuals "
            f"detected out of {len(unique_map):,} unique names",
        )
        return individual_count

    # ----- Stage 2: Token Grouping -----

    def _build_token_groups(
        self, unique_names: list[UniqueNameEntry]
    ) -> tuple[list[tuple[str, list]], list[tuple[str, list]]]:
        """
        Build token groups, split into LLM groups and singleton groups.
        Returns (llm_groups, singleton_groups) each as list of (key, members).
        """
        self._log("Stage 2: Token-based grouping (O(n) blocking)...")

        groups = build_groups(unique_names)

        llm_groups = [
            (k, members)
            for k, members in groups.items()
            if len(members) >= self.min_group_size
        ]
        # Sort by size descending
        llm_groups.sort(key=lambda x: len(x[1]), reverse=True)

        singleton_groups = [
            (k, members)
            for k, members in groups.items()
            if len(members) < self.min_group_size
        ]

        self._log(f"Created {len(groups):,} token groups")
        self._log(
            f"-> {len(llm_groups)} groups with {self.min_group_size}+ members (will send to LLM)"
        )
        self._log(f"-> {len(singleton_groups)} singleton/small groups (auto-pass)")

        # Log top 5 largest groups
        for key, members in llm_groups[:5]:
            self._log(f'  Top group: "{key}" - {len(members)} names')

        return llm_groups, singleton_groups

    # ----- Stage 3: LLM Clustering -----

    def _process_singletons(self, singleton_groups: list[tuple[str, list]]) -> list[NormalizationResult]:
        """Process singleton groups (no API needed)."""
        results = []
        for _key, members in singleton_groups:
            for m in members:
                entity_class = classify_entity(m["original"])
                results.append(NormalizationResult(
                    original=m["original"],
                    normalized=m["original"],  # Keep as-is
                    individual=entity_class.type == "individual",
                    cluster=f"S-{m['cleaned'][:20]}",
                    confidence="auto",
                    method="singleton",
                    count=m["count"],
                ))
        return results

    def _process_llm_batch(
        self,
        batch: list[dict],
        group_key: str,
        batch_idx: int,
        clusters_found: int,
    ) -> tuple[list[NormalizationResult], int, int]:
        """
        Send a single batch to Gemini and parse results.
        Returns (results, new_clusters_found, api_calls_made).
        """
        results = []
        batch_names = [m["original"] for m in batch]
        retries = 0
        api_calls = 0

        while retries < self.max_retries:
            try:
                api_calls += 1
                result = call_gemini(batch_names, self.api_key, self.model)

                if result and "clusters" in result:
                    assigned_indices: set[int] = set()

                    for cluster in result["clusters"]:
                        canonical = cluster.get("canonical") or batch_names[cluster.get("members", [0])[0]] or "Unknown"
                        member_indices = cluster.get("members", [])
                        conf = cluster.get("confidence", "medium")

                        # Only count as a real cluster if >1 member
                        if len(member_indices) > 1:
                            clusters_found += 1
                            
                        for idx in member_indices:
                            if 0 <= idx < len(batch) and idx not in assigned_indices:
                                assigned_indices.add(idx)
                                entity_class = classify_entity(batch[idx]["original"])
                                results.append(NormalizationResult(
                                    original=batch[idx]["original"],
                                    normalized=canonical,
                                    individual=entity_class.type == "individual",
                                    cluster=f"C-{clusters_found}",
                                    confidence=conf,
                                    method="llm-cluster" if len(member_indices) > 1 else "llm-singleton",
                                    count=batch[idx]["count"],
                                ))
                else:
                    assigned_indices = set()

                # Recover any names Gemini forgot to assign
                for mi in range(len(batch)):
                    if mi not in assigned_indices:
                        entity_class = classify_entity(batch[mi]["original"])
                        results.append(NormalizationResult(
                            original=batch[mi]["original"],
                            normalized=batch[mi]["original"],
                            individual=entity_class.type == "individual",
                            cluster=f"S-{batch[mi]['cleaned'][:20]}",
                            confidence="auto",
                            method="llm-missed",
                            count=batch[mi]["count"],
                        ))

                return results, clusters_found, api_calls

            except Exception as err:
                retries += 1
                err_msg = str(err)

                if "429" in err_msg or "quota" in err_msg:
                    wait = retries * 10
                    self._log(
                        f"Rate limited. Waiting {wait}s... (retry {retries}/{self.max_retries})",
                        "warning",
                    )
                    time.sleep(wait)
                elif retries < self.max_retries:
                    self._log(
                        f'Error on group "{group_key}" batch {batch_idx + 1}: {err_msg}. Retrying...',
                        "warning",
                    )
                    time.sleep(2)
                else:
                    self._log(
                        f'Failed group "{group_key}" after {self.max_retries} retries: {err_msg}',
                        "error",
                    )
                    # Fallback: keep originals
                    for m in batch:
                        entity_class = classify_entity(m["original"])
                        results.append(NormalizationResult(
                            original=m["original"],
                            normalized=m["original"],
                            individual=entity_class.type == "individual",
                            cluster="ERR",
                            confidence="error",
                            method="fallback",
                            count=m["count"],
                        ))
                    return results, clusters_found, api_calls

        return results, clusters_found, api_calls

    def _process_llm_groups(
        self, llm_groups: list[tuple[str, list]]
    ) -> tuple[list[NormalizationResult], int, int, int]:
        """
        Process all LLM groups through Gemini.
        Returns (results, clusters_found, total_api_calls, errors).
        """
        self._log("Stage 3: Sending groups to Gemini for entity clustering...")

        results = []
        clusters_found = 0
        total_api_calls = 0
        errors = 0
        total_names = sum(len(members) for _, members in llm_groups)
        names_processed = 0

        for gi, (group_key, members) in enumerate(llm_groups):
            # Split large groups into batches
            batches = [
                members[i : i + self.batch_size]
                for i in range(0, len(members), self.batch_size)
            ]

            for bi, batch in enumerate(batches):
                batch_results, clusters_found, api_calls = self._process_llm_batch(
                    batch, group_key, bi, clusters_found
                )
                results.extend(batch_results)
                total_api_calls += api_calls

                # Check if there were fallback results (errors)
                if any(r.method == "fallback" for r in batch_results):
                    errors += 1

                names_processed += len(batch)
                pct = names_processed / max(total_names, 1) * 100

                if gi % 3 == 0 or bi == len(batches) - 1:
                    self._log(
                        f'Group "{group_key}" ({len(members)} names) - '
                        f"batch {bi + 1}/{len(batches)} ✓ [{pct:.0f}%]",
                    )

                # Small delay between calls to respect rate limits
                if gi < len(llm_groups) - 1 or bi < len(batches) - 1:
                    time.sleep(self.delay_between_calls)

        return results, clusters_found, total_api_calls, errors

    # ----- Main Pipeline -----

    def normalize(self, raw_names: list[str]) -> list[NormalizationResult]:
        """
        Run the full normalization pipeline on a list of raw supplier names.

        Args:
            raw_names: List of raw supplier name strings.

        Returns:
            List of NormalizationResult objects.
        """
        # Stage 1: Extract & Clean
        unique_map = self._extract_and_clean(raw_names)

        # Stage 1.5: Entity Classification
        self._classify_entities(unique_map)

        # Build list of unique name entries
        unique_names = list(unique_map.values())

        # Stage 2: Token Grouping
        llm_groups, singleton_groups = self._build_token_groups(unique_names)

        # Stage 3: LLM Clustering
        # Process singletons first (no API needed)
        all_results = self._process_singletons(singleton_groups)

        # Process LLM groups
        llm_results, clusters_found, api_calls, errors = self._process_llm_groups(llm_groups)
        all_results.extend(llm_results)

        # Summary
        llm_clustered = sum(1 for r in all_results if r.method == "llm-cluster")
        individuals = sum(1 for r in all_results if r.individual)

        self._log("=" * 50)
        self._log("NORMALIZATION COMPLETE")
        self._log(f"Total rows: {len(raw_names):,}")
        self._log(f"Unique names: {len(unique_map):,}")
        self._log(f"Clusters found: {clusters_found:,}")
        self._log(f"LLM clustered: {llm_clustered:,}")
        self._log(f"Individuals detected: {individuals:,}")
        self._log(f"API calls made: {api_calls}")
        if errors > 0:
            self._log(f"Errors: {errors}", "error")
        self._log("=" * 50)

        return all_results

    def normalize_csv(
        self,
        csv_path: str,
        supplier_column: Optional[str] = None,
        supplier_column_index: Optional[int] = None,
        encoding: str = "utf-8",
    ) -> list[NormalizationResult]:
        """
        Read a CSV file and normalize supplier names.

        Args:
            csv_path: Path to the CSV file.
            supplier_column: Name of the column containing supplier names.
            supplier_column_index: Index of the column (0-based) if name not provided.
            encoding: File encoding (default: utf-8).

        Returns:
            List of NormalizationResult objects.
        """
        self._log(f"Reading CSV from {csv_path}...")

        with open(csv_path, "r", encoding=encoding) as f:
            reader = csv.reader(f)
            headers = next(reader, None) or []

            # Determine column index
            col_idx = supplier_column_index
            if supplier_column:
                # Exact match first
                for i, h in enumerate(headers):
                    if h.strip() == supplier_column:
                        col_idx = i
                        break

                # Case-insensitive fallback
                if col_idx is None:
                    for i, h in enumerate(headers):
                        if h.strip().lower() == supplier_column.lower():
                            col_idx = i
                            break
                if col_idx is None:
                    raise ValueError(
                        f"Column '{supplier_column}' not found in headers: {headers}"
                    )
            elif col_idx is None:
                # Auto-detect: look for 'supplier', 'vendor', 'name' in headers
                for i, h in enumerate(headers):
                    if re.search(r"supplier|vendor|name", h, re.IGNORECASE):
                        col_idx = i
                        break
                if col_idx is None:
                    col_idx = 0  # Default to first column

            header_label = headers[col_idx].strip() if col_idx < len(headers) else f"index_{col_idx}"
            self._log(f"Using column {col_idx}: '{header_label}'")

            raw_names: list[str] = []
            for row in reader:
                if col_idx < len(row):
                    raw_names.append(row[col_idx])
                else:
                    raw_names.append("")

            self._log(f"Loaded {len(raw_names):,} rows from CSV")
            return self.normalize(raw_names)

# ----- CSV Export -----

    @staticmethod
    def export_full_csv(results: list[NormalizationResult], output_path: str):
        """Export full results CSV with all columns."""
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Original Name", "Normalized Name", "Individual",
                "Cluster", "Confidence", "Method", "Row Count",
            ])
            for r in results:
                writer.writerow([
                    r.original, r.normalized, "Yes" if r.individual else "No",
                    r.cluster, r.confidence, r.method, r.count,
                ])
        logger.info(f"Full CSV exported: {output_path} ({len(results):,} rows)")

    @staticmethod
    def export_mapping_csv(results: list[NormalizationResult], output_path: str):
        """Export mapping-only CSV: Original Name + Normalized Name + Individual flag."""
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Original Name", "Normalized Name", "Individual"])
            for r in results:
                writer.writerow([r.original, r.normalized, "Yes" if r.individual else "No"])
        logger.info(f"Mapping CSV exported: {output_path} ({len(results):,} rows)")

    @staticmethod
    def export_clusters_csv(results: list[NormalizationResult], output_path: str):
        """Export clusters summary CSV (only multi-member clusters)."""
        clusters: dict[str, list[NormalizationResult]] = defaultdict(list)
        for r in results:
            clusters[r.normalized].append(r)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Canonical Name", "Individual", "Variant Count", "Variants", "Total Rows",
            ])
            for canonical, members in clusters.items():
                if len(members) > 1:
                    variants = " | ".join(m.original for m in members)
                    total_rows = sum(m.count for m in members)
                    is_individual = "Yes" if any(m.individual for m in members) else "No"
                    writer.writerow([canonical, is_individual, len(members), variants, total_rows])
        logger.info(f"Clusters CSV exported: {output_path}")

# ================================================================
# CLI ENTRY POINT
# ================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Supplier Name Normalizer | LLM-First Architecture"
    )
    parser.add_argument("--input", "-i", required=True, help="Input CSV file path")
    parser.add_argument("--column", "-c", help="Supplier name column header")
    parser.add_argument("--column-index", type=int, help="Supplier name column index (0-based)")
    parser.add_argument("--api-key", "-k", required=True, help="Gemini API key")
    parser.add_argument("--model", "-m", default="gemini-2.5-flash",
                        choices=["gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.0-flash"],
                        help="Gemini model to use")
    parser.add_argument("--batch-size", type=int, default=50, help="Names per API call")
    parser.add_argument("--min-group-size", type=int, default=2, help="Min group size to send to LLM")
    parser.add_argument("--output-dir", "-o", default=".", help="Output directory for CSV files")
    parser.add_argument("--encoding", default="utf-8", help="CSV file encoding")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )

    normalizer = SupplierNormalizer(
        api_key=args.api_key,
        model=args.model,
        batch_size=args.batch_size,
        min_group_size=args.min_group_size,
    )

    results = normalizer.normalize_csv(
        csv_path=args.input,
        supplier_column=args.column,
        supplier_column_index=args.column_index,
        encoding=args.encoding,
    )

    # Export all three CSVs
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    normalizer.export_full_csv(results, str(out_dir / "supplier_normalization_full.csv"))
    normalizer.export_mapping_csv(results, str(out_dir / "supplier_name_mapping.csv"))
    normalizer.export_clusters_csv(results, str(out_dir / "supplier_clusters_summary.csv"))

    # Print summary
    total = len(results)
    individuals = sum(1 for r in results if r.individual)
    clustered = sum(1 for r in results if r.method == "llm-cluster")
    print(f"\n{'='*50}")
    print(f" Names processed: {total:,}")
    print(f" LLM clustered: {clustered:,}")
    print(f" Individuals detected: {individuals:,}")
    print(f" Output directory: {out_dir.resolve()}")
    print(f"{'-'*50}")


if __name__ == "__main__":
    main()