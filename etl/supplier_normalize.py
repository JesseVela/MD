"""
Supplier name normalization and entity classification (from Bhavin's supplier_master_generator).
Used by supplier master ETL for: clean_name, get_group_key, classify_entity.
No GUI/tkinter/pandas dependency.
"""
import re
import unicodedata

# Legal suffixes to strip (from Bhavin's script)
LEGAL_SUFFIXES = {
    'inc', 'incorporated', 'corp', 'corporation', 'ltd', 'limited',
    'llc', 'llp', 'plc', 'pvt', 'private', 'gmbh', 'ag', 'sa', 'nv', 'bv',
    'co', 'company', 'group', 'holdings', 'holding', 'enterprises', 'enterprise',
    'services', 'service', 'solutions', 'solution', 'technologies', 'technology',
    'tech', 'systems', 'system', 'consulting', 'consultants', 'consultant',
    'partners', 'partner', 'associates', 'associate', 'industries', 'industry',
    'industrial', 'manufacturing', 'mfg', 'retail', 'wholesale', 'trading',
    'traders', 'trader', 'distributors', 'distributor', 'distribution',
    'logistics', 'supplies', 'supply', 'products', 'product', 'brands', 'brand',
    'pte', 'pty', 'sdn', 'bhd', 'kabushiki', 'kaisha', 'kk', 'ab', 'oy', 'as',
}
STRIP_PREFIX_WORDS = {'the', 'a', 'an', 'mr', 'mrs', 'ms', 'dr', 'prof', 'st', 'saint'}
STRONG_LEGAL_SUFFIXES = {
    'inc', 'incorporated', 'corp', 'corporation', 'llc', 'llp', 'ltd',
    'limited', 'plc', 'gmbh', 'ag', 'sa', 'sarl', 'sas', 'srl', 'bv', 'nv', 'pty', 'pvt',
}
PERSON_TITLES = {
    'mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'sir', 'dame', 'rev', 'reverend',
    'capt', 'captain', 'sgt', 'sergeant', 'lt', 'lieutenant', 'col', 'colonel',
    'gen', 'general', 'hon', 'honorable', 'judge', 'justice', 'rabbi', 'imam', 'pastor',
    'brother', 'sister', 'father', 'mother', 'jr', 'sr', 'ii', 'iii', 'iv',
}
PERSON_TITLE_SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv', 'esq', 'md', 'phd', 'dds', 'cpa', 'rn', 'pe'}
CORP_KEYWORDS = {
    'group', 'associates', 'partners', 'consulting', 'consultants', 'services', 'solutions',
    'technologies', 'technology', 'tech', 'systems', 'global', 'worldwide', 'enterprises',
    'industries', 'international', 'intl', 'holdings', 'holding', 'management', 'capital',
    'financial', 'insurance', 'logistics', 'supply', 'manufacturing', 'mfg', 'construction',
    'engineering', 'design', 'media', 'communications', 'network', 'networks', 'pharma',
    'software', 'digital', 'data', 'cloud', 'retail', 'wholesale', 'distribution', 'trading',
    'bank', 'trust', 'medical', 'healthcare', 'university', 'college', 'institute',
}
# Subset of first names for person vs org heuristic
FIRST_NAMES = {
    'james', 'john', 'robert', 'michael', 'william', 'david', 'richard', 'joseph', 'thomas', 'charles',
    'mary', 'patricia', 'jennifer', 'linda', 'barbara', 'elizabeth', 'susan', 'jessica', 'sarah', 'karen',
    'rahul', 'priya', 'amit', 'anita', 'sanjay', 'deepa', 'vikram', 'neha', 'rajesh', 'pooja',
    'carlos', 'maria', 'juan', 'ana', 'jose', 'luis', 'miguel', 'carmen', 'francisco', 'rosa',
    'wei', 'ming', 'jing', 'lei', 'yan', 'hui', 'ahmed', 'fatima', 'mohammed', 'ali',
    'bob', 'sam', 'tom', 'pat', 'sue', 'lee', 'max', 'ray', 'roy',
}
COMMON_FIRST_WORDS = {
    'american', 'national', 'united', 'general', 'first', 'new', 'north', 'south',
    'east', 'west', 'central', 'pacific', 'atlantic', 'great', 'royal', 'standard',
    'advanced', 'applied', 'best', 'blue', 'city', 'custom', 'digital', 'direct',
    'global', 'key', 'star', 'sun', 'top', 'total', 'us', 'usa', 'world', 'pro',
}


def clean_name(raw: str) -> str:
    """Deterministic name cleaning for grouping. Same logic as Bhavin's supplier_master_generator."""
    if not raw:
        return ''
    s = str(raw).strip()
    if re.match(r'^\W+\d+$', s) or len(s) < 2:
        return ''
    s = unicodedata.normalize('NFKD', s)
    s = re.sub(r'[\u0300-\u036f]', '', s)
    s = s.lower()
    s = s.replace('&', ' and ')
    s = re.sub(r'\bdba\b', '', s)
    s = re.sub(r'\bdb/b/a\b', '', s)
    s = re.sub(r'\baka\b', '', s)
    s = re.sub(r'\ba/k/a\b', '', s)
    s = re.sub(r'\bfka\b', '', s)
    s = re.sub(r'\bc/o\b', '', s)
    s = re.sub(r'\battn\b', '', s)
    s = re.sub(r'^\W+\s-', '', s)
    s = re.sub(r'\b\d{1,3}\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    tokens = s.split(' ')
    while len(tokens) > 1 and tokens[0] in STRIP_PREFIX_WORDS:
        tokens.pop(0)
    return ' '.join(tokens).strip()


def get_group_key(cleaned: str) -> str:
    """Token-based grouping key. Same as Bhavin's get_group_key."""
    if not cleaned:
        return '_empty_'
    tokens = cleaned.split(' ')
    key = tokens[0]
    if len(tokens) > 1 and (len(key) <= 2 or key in COMMON_FIRST_WORDS):
        key = f'{tokens[0]}_{tokens[1]}'
    if len(tokens) > 2 and len(key) <= 5:
        key = f'{key}_{tokens[2]}'
    return key


def classify_entity(raw_name: str) -> dict:
    """Classify name as individual or organization. Returns dict with type, confidence, reason."""
    if not raw_name:
        return {'type': 'unknown', 'confidence': 'low', 'reason': 'empty'}
    s = str(raw_name).strip()
    lower = re.sub(r'[^\w\s&]', ' ', s.lower())
    lower = re.sub(r'\s+', ' ', lower).strip()
    tokens = lower.split(' ')
    for t in tokens:
        if t in STRONG_LEGAL_SUFFIXES:
            return {'type': 'organization', 'confidence': 'high', 'reason': 'legal_suffix'}
    if tokens and tokens[0] in PERSON_TITLES:
        return {'type': 'individual', 'confidence': 'high', 'reason': 'title_prefix'}
    if tokens and tokens[-1] in PERSON_TITLE_SUFFIXES:
        return {'type': 'individual', 'confidence': 'high', 'reason': 'title_suffix'}
    for t in tokens:
        if t in CORP_KEYWORDS:
            return {'type': 'organization', 'confidence': 'high', 'reason': 'corp_keyword'}
    if re.search(r'[&+]', s) and len(tokens) >= 3:
        return {'type': 'organization', 'confidence': 'medium', 'reason': 'ampersand'}
    if re.search(r'\d', s):
        return {'type': 'organization', 'confidence': 'medium', 'reason': 'has_numbers'}
    if len(tokens) == 1:
        return {'type': 'organization', 'confidence': 'low', 'reason': 'single_word'}
    if len(tokens) >= 4:
        return {'type': 'organization', 'confidence': 'low', 'reason': 'long_name'}
    if 2 <= len(tokens) <= 3 and tokens[0] in FIRST_NAMES:
        return {'type': 'individual', 'confidence': 'medium', 'reason': 'firstname_match'}
    return {'type': 'unknown', 'confidence': 'low', 'reason': 'no_signal'}


def name_key_for_match(name: str) -> str:
    """Lowercase, strip, collapse spaces - for exact match key (e.g. against ref.supplier_master)."""
    if not name or not isinstance(name, str):
        return ''
    return ' '.join(name.lower().strip().split())
