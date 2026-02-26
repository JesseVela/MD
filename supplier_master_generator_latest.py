APP_VERSION = "4.4"
APP_BUILD = "2025-02-19"

import sys
import os
import json
import time
import re
import random
import threading
import queue
import unicodedata
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from difflib import SequenceMatcher

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
import requests

###########
# Country Code Map
###########
COUNTRY_CODES = {
    "AF": "Afghanistan", "AL": "Albania", "DZ": "Algeria", "AD": "Andorra",
    "AO": "Angola", "AR": "Argentina", "AM": "Armenia", "AU": "Australia",
    "AT": "Austria", "AZ": "Azerbaijan", "BS": "Bahamas", "BH": "Bahrain",
    "BD": "Bangladesh", "BB": "Barbados", "BY": "Belarus", "BE": "Belgium",
    "BZ": "Belize", "BJ": "Benin", "BT": "Bhutan", "BO": "Bolivia",
    "BA": "Bosnia and Herzegovina", "BW": "Botswana", "BR": "Brazil",
    "BN": "Brunei", "BG": "Bulgaria", "BF": "Burkina Faso", "BI": "Burundi",
    "KH": "Cambodia", "CM": "Cameroon", "CA": "Canada", "CV": "Cape Verde",
    "CF": "Central African Republic", "TD": "Chad", "CL": "Chile", "CN": "China",
    "CO": "Colombia", "KM": "Comoros", "CG": "Congo", "CD": "DR Congo",
    "CR": "Costa Rica", "CI": "Ivory Coast", "HR": "Croatia", "CU": "Cuba",
    "CY": "Cyprus", "CZ": "Czech Republic", "DK": "Denmark", "DJ": "Djibouti",
    "DM": "Dominica", "DO": "Dominican Republic", "EC": "Ecuador", "EG": "Egypt",
    "SV": "El Salvador", "GQ": "Equatorial Guinea", "ER": "Eritrea", "EE": "Estonia",
    "ET": "Ethiopia", "FJ": "Fiji", "FI": "Finland", "FR": "France", "GA": "Gabon",
    "GM": "Gambia", "GE": "Georgia", "DE": "Germany", "GH": "Ghana", "GR": "Greece",
    "GD": "Grenada", "GT": "Guatemala", "GN": "Guinea", "GW": "Guinea-Bissau",
    "GY": "Guyana", "HT": "Haiti", "HN": "Honduras", "HK": "Hong Kong",
    "HU": "Hungary", "IS": "Iceland", "IN": "India", "ID": "Indonesia",
    "IR": "Iran", "IQ": "Iraq", "IE": "Ireland", "IL": "Israel", "IT": "Italy",
    "JM": "Jamaica", "JP": "Japan", "JO": "Jordan", "KZ": "Kazakhstan",
    "KE": "Kenya", "KI": "Kiribati", "KP": "North Korea", "KR": "South Korea",
    "KW": "Kuwait", "KG": "Kyrgyzstan", "LA": "Laos", "LV": "Latvia",
    "LB": "Lebanon", "LS": "Lesotho", "LR": "Liberia", "LY": "Libya",
    "LI": "Liechtenstein", "LT": "Lithuania", "LU": "Luxembourg", "MO": "Macau",
    "MK": "North Macedonia", "MG": "Madagascar", "MW": "Malawi", "MY": "Malaysia",
    "MV": "Maldives", "ML": "Mali", "MT": "Malta", "MH": "Marshall Islands",
    "MR": "Mauritania", "MU": "Mauritius", "MX": "Mexico", "FM": "Micronesia",
    "MD": "Moldova", "MC": "Monaco", "MN": "Mongolia", "ME": "Montenegro",
    "MA": "Morocco", "MZ": "Mozambique", "MM": "Myanmar", "NA": "Namibia",
    "NR": "Nauru", "NP": "Nepal", "NL": "Netherlands", "NZ": "New Zealand",
    "NI": "Nicaragua", "NE": "Niger", "NG": "Nigeria", "NO": "Norway",
    "OM": "Oman", "PK": "Pakistan", "PW": "Palau", "PS": "Palestine",
    "PA": "Panama", "PG": "Papua New Guinea", "PY": "Paraguay", "PE": "Peru",
    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "PR": "Puerto Rico",
    "QA": "Qatar", "RO": "Romania", "RU": "Russia", "RW": "Rwanda",
    "KN": "Saint Kitts and Nevis", "LC": "Saint Lucia", "VC": "Saint Vincent",
    "WS": "Samoa", "SM": "San Marino", "ST": "Sao Tome and Principe",
    "SA": "Saudi Arabia", "SN": "Senegal", "RS": "Serbia", "SC": "Seychelles",
    "SL": "Sierra Leone", "SG": "Singapore", "SK": "Slovakia", "SI": "Slovenia",
    "SB": "Solomon Islands", "SO": "Somalia", "ZA": "South Africa", "SS": "South Sudan",
    "ES": "Spain", "LK": "Sri Lanka", "SD": "Sudan", "SR": "Suriname",
    "SZ": "Eswatini", "SE": "Sweden", "CH": "Switzerland", "SY": "Syria",
    "TW": "Taiwan", "TJ": "Tajikistan", "TZ": "Tanzania", "TH": "Thailand",
    "TL": "Timor-Leste", "TG": "Togo", "TO": "Tonga", "TT": "Trinidad and Tobago",
    "TN": "Tunisia", "TR": "Turkey", "TM": "Turkmenistan", "TV": "Tuvalu",
    "UG": "Uganda", "UA": "Ukraine", "AE": "United Arab Emirates",
    "GB": "United Kingdom", "UK": "United Kingdom", "US": "United States",
    "USA": "United States", "UY": "Uruguay", "UZ": "Uzbekistan", "VU": "Vanuatu",
    "VA": "Vatican City", "VE": "Venezuela", "VN": "Vietnam", "YE": "Yemen",
    "ZM": "Zambia", "ZW": "Zimbabwe"
}

COUNTRY_NAME_TO_CODE = {v.upper(): k for k, v in COUNTRY_CODES.items()}

# Legal suffixes to strip
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

# --- Prefix words to strip from name start ---
STRIP_PREFIX_WORDS = {'the', 'a', 'an', 'mr', 'mrs', 'ms', 'dr', 'prof', 'st', 'saint'}

# --- First name dictionary (~650 unique names across cultures) ---
FIRST_NAMES = {
    # English - Male
    'james', 'john', 'robert', 'michael', 'william', 'david', 'richard', 'joseph', 'thomas', 'charles',
    'christopher', 'daniel', 'matthew', 'anthony', 'mark', 'donald', 'steven', 'paul', 'andrew', 'joshua',
    'kenneth', 'kevin', 'brian', 'george', 'timothy', 'ronald', 'edward', 'jason', 'jeffrey', 'ryan',
    'jacob', 'gary', 'nicholas', 'eric', 'jonathan', 'stephen', 'larry', 'justin', 'scott', 'brandon',
    'benjamin', 'samuel', 'raymond', 'gregory', 'frank', 'alexander', 'patrick', 'jack', 'dennis', 'jerry',
    'tyler', 'aaron', 'jose', 'adam', 'nathan', 'henry', 'peter', 'zachary', 'douglas', 'harold',
    'kyle', 'noah', 'gerald', 'carl', 'keith', 'roger', 'jeremy', 'terry', 'sean', 'austin',
    'arthur', 'lawrence', 'jesse', 'dylan', 'bryan', 'joe', 'jordan', 'billy', 'bruce', 'albert',
    'willie', 'gabriel', 'logan', 'ralph', 'eugene', 'russell', 'bobby', 'mason', 'philip', 'louis',
    'wayne', 'randy', 'vincent', 'liam', 'ethan', 'aiden', 'owen', 'luke', 'connor', 'ian',
    # English - Female
    'mary', 'patricia', 'jennifer', 'linda', 'barbara', 'elizabeth', 'susan', 'jessica', 'sarah', 'karen',
    'lisa', 'nancy', 'betty', 'margaret', 'sandra', 'ashley', 'dorothy', 'kimberly', 'emily', 'donna',
    'michelle', 'carol', 'amanda', 'melissa', 'deborah', 'stephanie', 'rebecca', 'sharon', 'laura', 'cynthia',
    'kathleen', 'amy', 'angela', 'shirley', 'anna', 'brenda', 'pamela', 'emma', 'nicole', 'helen',
    'samantha', 'katherine', 'christine', 'debra', 'rachel', 'carolyn', 'janet', 'catherine', 'maria', 'heather',
    'diane', 'ruth', 'julie', 'olivia', 'joyce', 'virginia', 'victoria', 'kelly', 'lauren', 'christina',
    'joan', 'evelyn', 'judith', 'megan', 'andrea', 'cheryl', 'hannah', 'jacqueline', 'martha', 'gloria',
    'teresa', 'ann', 'sara', 'madison', 'frances', 'kathryn', 'janice', 'jean', 'abigail', 'alice',
    'julia', 'judy', 'sophia', 'grace', 'denise', 'amber', 'doris', 'marilyn', 'danielle', 'beverly',
    'isabella', 'theresa', 'diana', 'natalie', 'brittany', 'charlotte', 'marie', 'kayla', 'alexis', 'lori',
    # Indian
    'aarav', 'aditya', 'akash', 'akshay', 'akshaye', 'amit', 'amitabh', 'anil', 'ankit', 'anurag',
    'arjun', 'arun', 'ashish', 'bharat', 'chandra', 'deepak', 'dev', 'dhruv', 'dinesh', 'ganesh',
    'gaurav', 'gopal', 'hari', 'harsh', 'hemant', 'ishaan', 'jagdish', 'jay', 'karan', 'kartik',
    'krishna', 'kumar', 'lalit', 'mahesh', 'manoj '4 'mohit', 'mukesh', 'naman', 'naresh', 'nikhil',
    'nitin', 'pankaj', 'pranav', 'prashant', 'rahul', 'raj', 'rajesh', 'rajan', 'rakesh', 'ravi',
    'rohit', 'sachin', 'sanjay', 'sanjeev', 'satish', 'shiv', 'shyam', 'siddharth', 'sunil', 'suresh',
    'tushar', 'varun', 'vijay', 'vikram', 'vinay', 'vinod', 'vipin', 'vishal', 'vivek', 'yash',
    'aarti', 'ananya', 'anjali', 'anita', 'asha', 'bhavna', 'chitra', 'deepa', 'divya', 'durga',
    'gayatri', 'geeta', 'isha', 'jaya', 'jyoti', 'kajal', 'kavita', 'komal', 'lata', 'madhuri',
    'mamta', 'meena', 'megha', 'mira', 'nandini', 'neha', 'nisha', 'nita', 'padma', 'pallavi',
    'pooja', 'priya', 'radha', 'rashmi', 'rekha', 'renu', 'rina', 'ritu', 'roshni', 'sakshi',
    'sangeeta', 'sarita', 'seema', 'shanti', 'shilpa', 'shreya', 'sita', 'smita', 'sneha', 'sonali',
    'sonia', 'sudha', 'sunita', 'sushma', 'swati', 'tanvi', 'uma', 'usha', 'vandana', 'vidya',
    # Chinese (romanized)
    'wei', 'fang', 'lei', 'jing', 'ling', 'yan', 'hui', 'xin',
    'ming', 'hong', 'ping', 'chun', 'dong', 'feng', 'hai', 'jie', 'jun', 'kai',
    'bin', 'bo', 'chang', 'cheng', 'gang', 'guo', 'hao', 'hua', 'jian', 'liang',
    'lin', 'long', 'peng', 'qiang', 'rong', 'shan', 'tao', 'wen', 'xiang', 'yi',
    'yong', 'yuan', 'yun', 'zhi', 'zhong',
    # Hispanic
    'alejandro', 'alfredo', 'andres', 'angel', 'antonio', 'arturo', 'carlos', 'cesar', 'cristian', 'diego',
    'eduardo', 'enrique', 'ernesto', 'felipe', 'fernando', 'francisco', 'guillermo', 'gustavo', 'hector', 'hugo',
    'ignacio', 'ivan', 'javier', 'jesus', 'joaquin', 'jorge', 'juan', 'julio', 'leonardo', 'luis',
    'manuel', 'marco', 'marcos', 'mario', 'martin', 'miguel', 'nicolas', 'oscar', 'pablo', 'pedro',
    'rafael', 'ramon', 'raul', 'ricardo', 'rodrigo', 'ruben', 'salvador', 'santiago', 'sergio', 'victor',
    'adriana', 'alejandra', 'alicia', 'ana', 'beatriz', 'camila', 'carmen', 'catalina', 'claudia', 'daniela',
    'elena', 'fernanda', 'gabriela', 'guadalupe', 'isabel', 'jimena', 'lucia', 'luisa', 'margarita',
    'monica', 'natalia', 'paola', 'patricia', 'rosa', 'silvia', 'sofia', 'valentina', 'valeria', 'veronica',
    # Arabic
    'abdul', 'abdullah', 'ahmad', 'ahmed', 'ali', 'amir', 'bilal', 'farid', 'hamza', 'hasan',
    'hassan', 'hussein', 'ibrahim', 'imran', 'ismail', 'jamal', 'kareem', 'khalid', 'mahmoud', 'mansour',
    'mohammed', 'mohamad', 'muhammad', 'mustafa', 'nabil', 'nader', 'nasir', 'omar', 'rashid',
    'saeed', 'said', 'saleh', 'sami', 'tariq', 'walid', 'youssef', 'zaid',
    'aisha', 'amina', 'ayesha', 'fatima', 'hana', 'khadija', 'layla', 'leila', 'mariam',
    'maryam', 'nadia', 'noura', 'rania', 'reem', 'salma', 'yasmin', 'zahra',
    # European
    'anders', 'bjorn', 'erik', 'gustav', 'hans', 'henrik', 'ingrid', 'johan', 'karl', 'lars',
    'magnus', 'nils', 'olaf', 'sven', 'axel', 'astrid', 'brigitte', 'elsa',
    'franz', 'fritz', 'gerhard', 'gunther', 'heinrich', 'helmut', 'jurgen', 'klaus', 'ludwig',
    'manfred', 'otto', 'rolf', 'siegfried', 'ulrich', 'werner', 'wolfgang', 'claude', 'francois', 'jacques',
    'jean', 'marcel', 'philippe', 'pierre', 'rene', 'yves', 'andre', 'antoine',
    'benoit', 'christophe', 'dominique', 'etienne', 'florian', 'guillaume',
    'laurent', 'mathieu', 'olivier', 'pascal', 'raphael', 'sebastien', 'thierry',
    'giovanni', 'giuseppe', 'luca', 'matteo', 'paolo', 'roberto', 'angelo', 'bruno',
    'carlo', 'dario', 'fabio', 'giorgio', 'lorenzo', 'massimo', 'nicola', 'pietro', 'stefano',
    # Korean/Japanese romanized
    'hyun', 'jin', 'sang', 'seung', 'soo', 'sung', 'won', 'young',
    'akira', 'haruki', 'hiroshi', 'kenji', 'makoto', 'naoki', 'satoshi', 'takeshi',
    'yuki', 'haruka', 'keiko', 'megumi', 'sakura', 'yumi',
    # Common short names
    'bob', 'rob', 'ted', 'ed', 'al', 'ben', 'dan', 'don', 'jim', 'jon', 'sam', 'tim', 'tom', 'pat', 'sue', 'lee',
    'max', 'ray', 'roy', 'rex', 'bud', 'hal', 'ned', 'walt', 'hank', 'chuck', 'rick', 'nick', 'mike', 'dave',
    'steve', 'chris', 'matt', 'jeff', 'greg', 'craig', 'brad', 'chad', 'derek', 'troy', 'wade', 'dean', 'dale',
}

# --- Person title prefixes/suffixes
PERSON_TITLES = {
'mr', 'mrs', 'ms', 'miss', 'dr', 'prof', 'sir', 'dame', 'rev', 'reverend',
'capt', 'captain', 'sgt', 'sergeant', 'lt', 'lieutenant', 'col', 'colonel',
'gen', 'general', 'hon', 'honorable', 'judge', 'justice', 'rabbi', 'imam', 'pastor',
'brother', 'sister', 'father', 'mother', 'jr', 'sr', 'ii', 'iii', 'iv',
}
PERSON_TITLE_SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv', 'esq', 'md', 'phd', 'dds', 'cpa', 'rn', 'pe'}

# --- Corporate keywords (strong org indicators)
CORP_KEYWORDS = {
    'group', 'associates', 'partners', 'consulting', 'consultants', 'services', 'solutions',
    'technologies', 'technology', 'tech', 'systems', 'global', 'worldwide', 'enterprises',
    'industries', 'international', 'intl', 'holdings', 'holding', 'management', 'capital',
    'financial', 'insurance', 'logistics', 'supply', 'manufacturing', 'mfg', 'construction',
    'engineering', 'design', 'media', 'communications', 'network', 'networks', 'pharma',
    'pharmaceutical', 'medical', 'healthcare', 'health', 'clinic', 'hospital', 'university',
    'college', 'institute', 'foundation', 'society', 'association', 'federation', 'council',
    'bureau', 'agency', 'authority', 'department', 'division',
    'bank', 'trust', 'fund', 'realty', 'property', 'properties', 'development',
    'foods', 'food', 'beverage', 'restaurant', 'hotel', 'resort',
    'airlines', 'airways', 'motors', 'auto', 'automotive', 'electric', 'energy', 'power',
    'petroleum', 'oil', 'gas', 'mining', 'metals', 'steel', 'chemical', 'chemicals',
    'textiles', 'apparel', 'furniture', 'equipment', 'tools', 'hardware',
    'software', 'digital', 'data', 'cloud', 'cyber', 'security', 'defense', 'defence',
    'transport', 'transportation', 'freight', 'shipping', 'marine', 'aviation',
    'telecom', 'telecommunications', 'wireless', 'publishing',
    'studio', 'studios', 'entertainment', 'gaming',
    'retail', 'wholesale', 'distribution', 'distributors', 'imports', 'exports', 'trading',
    'ventures', 'innovations', 'labs', 'laboratory', 'laboratories', 'research',
    'cooperative', 'coop', 'union', 'works', 'factory', 'plant', 'mill',
    'store', 'stores', 'shop', 'shops', 'market', 'markets', 'mart', 'plaza', 'center', 'centre',
    'depot', 'warehouse', 'exchange',
}

# Strong legal suffixes (definitive org indicators for entity classification)
STRONG_LEGAL_SUFFIXES = {
'inc', 'incorporated', 'corp', 'corporation', 'llc', 'llp', 'ltd',
'limited', 'plc', 'gmbh', 'ag', 'sa', 'sarl', 'sas', 'srl', 'bv', 'nv', 'pty', 'pvt',
}

# --- Common first words that create mega-groups (use 2-token keys) --
COMMON_FIRST_WORDS = {
    'american', 'national', 'united', 'general', 'first', 'new', 'north', 'south',
    'east', 'west', 'central', 'pacific', 'atlantic', 'great', 'royal', 'standard',
    'advanced', 'applied', 'best', 'blue', 'city', 'classic', 'complete', 'creative',
    'custom', 'digital', 'direct', 'elite', 'express', 'five', 'four', 'golden',
    'green', 'high', 'home', 'ideal', 'key', 'all', 'star', 'sun', 'top', 'total',
    'tri', 'true', 'twin', 'us', 'usa', 'white', 'world', 'pro', 'premier', 'prime',
}

# Extended legal suffixes for OUTPUT cleaning
LEGAL_SUFFIXES_OUTPUT = [
    r'\bprivate\s+limited\b',
    r'\bpvt\.?\s*ltd\.?\b',
    r'\bpte\.?\s*ltd\.?\b',
    r'\bsociedad\s+anonima\b',
    r'\bsp\.?\s*z\.?\s*o\.?\s*o\.?\b',
    r'\bsdn\.?\s*bhd\.?\b',
    r'\bpty\.?\s*ltd\.?\b',
    r'\bnaamloze\s+vennootschap\b',
    r'\bbesloten\s+vennootschap\b',
    r'\bincorporated\b',
    r'\bcorporation\b',
    r'\blimited\b',
    r'\bcompany\b',
    r'\bgmbh\b',
    r'\bcorp\.?\b',
    r'\binc\.?\b',
    r'\bltd\.?\b',
    r'\bllc\.?\b',
    r'\bllp\.?\b',
    r'\bplc\.?\b',
    r'\bpvt\.?\b',
    r'\bpte\.?\b',
    r'\bpty\.?\b',
    r'\bs\.?r\.?l\.?\b',
    r'\bs\.?r\.?l\.?\b',
    r'\bco\.?\b',
]

LOCATION_WORDS_OUTPUT = {
    'singapore', 'india', 'indian', 'china', 'chinese', 'japan', 'japanese',
    'korea', 'korean', 'taiwan', 'thailand', 'vietnam', 'indonesia', 'malaysia',
    'philippines', 'australia', 'australian', 'uk', 'usa', 'us', 'canada',
    'canadian', 'mexico', 'mexican', 'brazil', 'brazilian', 'germany', 'german',
    'france', 'french', 'italy', 'italian', 'spain', 'spanish', 'netherlands',
    'dutch', 'belgium', 'swiss', 'switzerland', 'austria', 'austrian',
    'ireland', 'irish', 'sweden', 'swedish', 'norway', 'norwegian', 'denmark',
    'danish', 'finland', 'finnish', 'polish', 'poland', 'russian', 'russia',
    'british', 'american', 'bermuda', 'hong', 'kong', 'hongkong',
}


###########
# Rate Limiter
###########
class RateLimiter:
    """Thread-safe rate limiter for API calls"""
    
    def __init__(self, max_rpm: int = 30):
        self.max_rpm = max_rpm
        self.request_times = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            self.request_times = [t for t in self.request_times if now - t < 60]
            
            if len(self.request_times) >= self.max_rpm:
                        wait_time = 60 - (now - self.request_times[0]) + 0.1
            if wait_time > 0:
                time.sleep(wait_time)
                now = time.time()
            self.request_times = [t for t in self.request_times if now - t < 60]
        
        self.request_times.append(time.time())


###########
# Utility Functions
###########
def normalize_country(country_value: str) -> Tuple[str, str]:
    """Normalize country value to standard name and code"""
    if not country_value or pd.isna(country_value):
        return ("Unknown", "XX")
    
    country_value = str(country_value).strip().upper()
    
    if country_value in COUNTRY_CODES:
        return (COUNTRY_CODES[country_value], country_value)
    
    if country_value in COUNTRY_NAME_TO_CODE:
        code = COUNTRY_NAME_TO_CODE[country_value]
        return (COUNTRY_CODES[code], code)
    
    for code, name in COUNTRY_CODES.items():
        if country_value in name.upper() or name.upper() in country_value:
            return (name, code)
    
    return (country_value.title(), "XX")


def safe_extract_json(text: str) -> Optional[Dict]:
    """Safely extract JSON from text response"""
    if not text:
        return None
    
    def ensure_dict(obj):
        if isinstance(obj, dict):
            return obj
        elif isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
            return obj[0]
        return None
    
    try:
        parsed = json.loads(text)
        return ensure_dict(parsed)
    except:
        pass
    
    json_match = re.search(r'```json\s*([\s\S]*?)\s*```', text)
    if json_match:
        try:
            parsed = json.loads(json_match.group(1))
            return ensure_dict(parsed)
        except:
            pass
    
    object_match = re.search(r'\{[\s\S]*\}', text)
    if object_match:
        try:
            parsed = json.loads(object_match.group(0))
            return ensure_dict(parsed)
        except:
            pass
    
    return None


def prefilter_categories(description: str, categories: List[Dict], k: int = 25) -> List[Dict]:
    """Pre-filter categories based on token overlap with L1, L2, and L3"""
    def get_tokens(text: str) -> set:
        return set(re.split(r'\W+', text.lower()))
    
    desc_tokens = get_tokens(description)
    
    scored = []
    for cat in categories:
        # Combine L1, L2, L3 for matching
        cat_text = f"{cat.get('l1', '')} {cat.get('l2', '')} {cat.get('l3', '')}"
        cat_tokens = get_tokens(cat_text)
        score = 0
        
        for token in desc_tokens:
            if len(token) <= 2:
                continue
            if token in cat_tokens:
                score += 1
            for cat_token in cat_tokens:
                if len(cat_token) <= 2:
                    continue
                if token in cat_token or cat_token in token:
                    score += 0.5
        
        scored.append({**cat, '_score': score})
    
    scored.sort(key=lambda x: x['_score'], reverse=True)
    return [{k: v for k, v in cat.items() if k != '_score'} for cat in scored[:k]]


def merge_tags(existing_tags: str, new_tags: List[str]) -> str:
    """Merge existing tags with new tags, avoiding duplicates"""
    if not existing_tags or pd.isna(existing_tags):
        existing_list = []
    else:
        existing_list = [t.strip() for t in str(existing_tags).split(',') if t.strip()]
    
    existing_lower = set([t.lower() for t in existing_list])
    
    final_tags = existing_list.copy()
    for tag in new_tags:
        if tag.strip() and tag.strip().lower() not in existing_lower:
            final_tags.append(tag.strip())
            existing_lower.add(tag.strip().lower())
    
    return ', '.join(final_tags) if final_tags else ''


def merge_locations(existing_locations: str, new_locations: List[str]) -> str:
    """Merge existing locations with new locations, avoiding duplicates"""
    if not existing_locations or pd.isna(existing_locations):
        existing_set = set()
    else:
        existing_set = set([l.strip() for l in str(existing_locations).split(',')
                          if l.strip() and l.strip() != 'Unknown'])
    
    new_set = set([l.strip() for l in new_locations if l.strip() and l.strip() != 'Unknown'])
    
    all_locations = list(existing_set | new_set)
    return ', '.join(sorted(all_locations)) if all_locations else 'Unknown'


###########
# PO Line Quality Check Functions
###########
def check_po_line_quality(description: str) -> bool:
    """
    Check if a PO line description is of sufficient quality for classification.
    
    Returns True if the description passes quality checks, False otherwise.
    
    Heuristics:
    1. Fail if description is all numbers (not descriptive)
    2. Fail if length < 5 characters
    """
    if not description or pd.isna(description):
        return False
    
    desc = str(description).strip()
    
    # Heuristic 1: Length check - must be at least 5 characters
    if len(desc) < 5:
        return False
    
    # Heuristic 2: All numbers check - remove spaces/punctuation and check if only digits
    # This catches things like "12345", "123-456-789", "12.34.56"
    digits_only = re.sub(r'[\s\.\-\/\\,()]+', '', desc)
    if digits_only.isdigit():
        return False
    
    return True


def assess_supplier_po_quality(item_descriptions: List[str]) -> Tuple[List[str], List[str], float]:
    """
    Assess the quality of PO line descriptions for a supplier.
    
    Returns:
        - passing_descriptions: List of descriptions that passed quality check
        - failing_descriptions: List of descriptions that failed quality check
        - pass_rate: Percentage of descriptions that passed (0.0 to 1.0)
    """
    if not item_descriptions:
        return [], [], 0.0

    passing = []
    failing = []
    
    for desc in item_descriptions:
        if check_po_line_quality(desc):
            passing.append(desc)
        else:
            failing.append(desc)
    
    total = len(item_descriptions)
    pass_rate = len(passing) / total if total > 0 else 0.0
    
    return passing, failing, pass_rate


###########
# Supplier Name Cleaning Functions
###########
def clean_canonical_name(name: str) -> str:
    """Clean a supplier name for OUTPUT (canonical name)."""
    if not name:
        return ""
    
    original = str(name).strip()
    result = original
    
    result = re.sub(r'\s*\([^)]*\)\s*', ' ', result)
    
    for suffix_pattern in LEGAL_SUFFIXES_OUTPUT:
        result = re.sub(suffix_pattern, ' ', result, flags=re.IGNORECASE)

    words = result.split()
    filtered_words = []
    for word in words:
        word_clean = re.sub(r'[^\w]', '', word).lower()
        if word_clean and word_clean not in LOCATION_WORDS_OUTPUT:
            filtered_words.append(word)
    result = ''.join(filtered_words)

    result = re.sub(r'[\s,-]+$', '', result)
    result = re.sub(r'^[\s,-]+', '', result)
    result = re.sub(r'\s+', ' ', result).strip()

    if not result or len(result) < 2:
        result = original
        result = re.sub(r'\s*\([^)]*\)\s*', ' ', result)
        for suffix_pattern in LEGAL_SUFFIXES_OUTPUT:
            result = re.sub(suffix_pattern, ' ', result, flags=re.IGNORECASE)
        result = re.sub(r'\s+', ' ', result).strip()
        result = re.sub(r'[\s,-]+$', '', result)
    
    if not result or len(result) < 2:
        result = original
    
    if result == result.upper() or result == result.lower():
        words = result.split()
        capitalized = []
        for word in words:
            if len(word) <= 4 and re.match(r'^[A-Z0-9&]+$', word):
                capitalized.append(word.upper())
            elif '&' in word:
                capitalized.append(word.upper())
            else:
                capitalized.append(word.capitalize())
        result = ' '.join(capitalized)
    
    return result


def clean_company_name(name: str) -> str:
    """Clean company name for comparison."""
    if not name:
        return ""
    
    name = str(name).lower().strip()
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'[^\w\s&]', ' ', name)
    name = re.sub(r'\s+&\s+', ' and ', name)
    
    tokens = name.split()
    
    filtered_tokens = []
    for token in tokens:
        token_clean = token.strip()
        if not token_clean:
            continue
        if token_clean in LEGAL_SUFFIXES:
            continue
        if token_clean in LOCATION_WORDS:
            continue
        if len(token_clean) == 1 and token_clean not in ('&',):
            continue
        filtered_tokens.append(token_clean)
    
    if not filtered_tokens:
        filtered_tokens = [t for t in tokens if t.strip() and t.strip() not in LEGAL_SUFFIXES]
    
    if not filtered_tokens:
        filtered_tokens = [t for t in tokens if t.strip()]
    
    return ' '.join(filtered_tokens)


def get_tokens(name: str) -> set:
    """Extract meaningful tokens from company name"""
    cleaned = clean_company_name(name)
    tokens = set([t for t in cleaned.split() if len(t) > 1])
    return tokens


def jaccard_similarity(set1: set, set2: set) -> float:
    """Calculate Jaccard similarity between two token sets"""
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def is_abbreviation(short: str, long: str) -> bool:
    """Check if 'short' is an abbreviation/acronym of 'long'"""
    short_clean = re.sub(r'[^\w]', '', short).upper()
    
    long_clean = long.lower()
    for suffix in ['inc', 'corp', 'ltd', 'llc', 'plc', 'pvt', 'limited', 'corporation']:
        long_clean = re.sub(r'\b' + suffix + r'\b', '', long_clean)
    long_clean = re.sub(r'[^\w\s]', ' ', long_clean).strip()
    long_tokens = [t for t in long_clean.split() if len(t) > 1]
    
    if not short_clean or not long_tokens:
        return False
    
    if len(short_clean) == len(long_tokens):
        initials = ''.join([t[0].upper() for t in long_tokens if t])
        if short_clean == initials:
            return True
    
    if len(short_clean) >= 2 and len(long_tokens) >= 2:
        for n in range(2, min(len(short_clean) + 1, len(long_tokens) + 1)):
            initials = ''.join([long_tokens[i][0].upper() for i in range(n) if i < len(long_tokens)])
            if short_clean.startswith(initials):
                return True
    
    if len(long_tokens) > 0 and len(short_clean) >= 3:
        first_word = long_tokens[0].upper()
        if first_word.startswith(short_clean[:3]):
            return True
    
    if len(long_tokens) >= 2 and len(short_clean) >= 4:
        short_lower = short_clean.lower()
        for split_point in range(2, len(short_lower) - 1):
            first_part = short_lower[:split_point]
            second_part = short_lower[split_point:]
            
            if (long_tokens[0].startswith(first_part) and
                len(long_tokens) > 1 and
                long_tokens[1].startswith(second_part)):
                return True
    
    return False


def levenshtein_ratio(s1: str, s2: str) -> float:
    """Calculate Levenshtein similarity ratio (0.0 to 1.0)"""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, str(s1).lower(), str(s2).lower()).ratio()


def token_sort_ratio(s1: str, s2: str) -> float:
    """Sort tokens alphabetically then compare"""
    tokens1 = ' '.join(sorted(clean_company_name(s1).split()))
    tokens2 = ' '.join(sorted(clean_company_name(s2).split()))
    return SequenceMatcher(None, tokens1, tokens2).ratio()


def company_similarity(name1: str, name2: str) -> float:
    """Multi-signal similarity score for company names."""
    if not name1 or not name2:
        return 0.0
    
    clean1 = clean_company_name(name1)
    clean2 = clean_company_name(name2)
    
    if clean1 == clean2:
        return 1.0
    
    if not clean1 or not clean2:
        return 0.0
    
    tokens1 = get_tokens(name1)
    tokens2 = get_tokens(name2)
    
    jaccard = jaccard_similarity(tokens1, tokens2)
    lev_ratio = levenshtein_ratio(clean1, clean2)
    token_sort = token_sort_ratio(name1, name2)
    
    abbrev_score = 0.0
    if len(clean1) <= 6 or len(clean2) <= 6:
        if is_abbreviation(name1, name2) or is_abbreviation(name2, name1):
            abbrev_score = 1.0
    
    score = (jaccard * 0.35) + (lev_ratio * 0.25) + (token_sort * 0.25) + (abbrev_score * 0.15)
    
    if tokens1 and tokens2:
        if tokens1.issubset(tokens2) or tokens2.issubset(tokens1):
            score = max(score, 0.85)
    
    if tokens1 and tokens2:
        main1 = max(tokens1, key=len) if tokens1 else ''
        main2 = max(tokens2, key=len) if tokens2 else ''
        if main1 and main2 and main1 == main2:
            score = max(score, 0.80)
    
    return min(score, 1.0)


def cluster_suppliers_algorithmic(names: List[str], threshold: float = 0.65) -> List[List[str]]:
    """Cluster similar supplier names using multi-signal similarity."""
    if not names:
        return []
    
    clusters = []
    
    for name in names:
        if not name or not str(name).strip():
            continue
        
        best_cluster_idx = None
        best_score = 0
        
        for idx, cluster in enumerate(clusters):
            for existing_name in cluster:
                score = company_similarity(name, existing_name)
                if score > best_score:
                    best_score = score
                    if score >= threshold:
                        best_cluster_idx = idx
        
        if best_cluster_idx is not None:
            clusters[best_cluster_idx].append(name)
        else:
            clusters.append([name])
    
    return clusters


def pick_canonical_name(names: List[str]) -> str:
    """Pick the best canonical name from a cluster."""
    names = [n for n in names if n and str(n).strip()]
    
    if not names:
        return "Unknown Supplier"
    if len(names) == 1:
        return names[0]
    
    REGIONAL_PATTERNS = [
        r'\([^)]*\)',
        r'\bpvt\b',
        r'\bprivate\b',
        r'\bindia\b',
        r'\buk\b',
        r'\busa\b',
        r'\basia\b',
        r'\beurope\b',
        r'\bglobal\b',
        r'\bregion\b',
    ]
    
    scored = []
    for name in names:
        score = 100
        
        name_lower = name.lower()
        for pattern in REGIONAL_PATTERNS:
            if re.search(pattern, name_lower, re.IGNORECASE):
                score -= 30
        
        if len(name) > 40:
            score -= 15
        elif len(name) > 30:
            score -= 10
        elif len(name) > 25:
            score -= 5
        
        if name != name.upper() and name != name.lower():
            score += 10
        
        if re.search(r'\b(Inc|Corp|Corporation)\b', name, re.IGNORECASE):
            score += 5
        
        if re.search(r'\bpvt\s*ltd\b', name_lower) or re.search(r'\bprivate\s*limited\b', name_lower):
            score -= 25
        
        score -= len(name) * 0.5
        
        scored.append((name, score))
    
    scored.sort(key=lambda x: x[1], reverse=True)
    
    best_name = scored[0][0]
    cleaned_name = clean_canonical_name(best_name)
    
    if not cleaned_name or len(cleaned_name) < 2:
        return best_name
    
    return cleaned_name


###########
# Genpact Supplier Master File Operations
###########
def load_genpact_supplier_master(file_path: str) -> pd.DataFrame:
    """Load existing Genpact Supplier Master from disk"""
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            return df
        except:
            try:
                df = pd.read_csv(file_path, encoding='latin-1')
                return df
            except Exception as e:
                return create_empty_genpact_sm()
    else:
        return create_empty_genpact_sm()


def create_empty_genpact_sm() -> pd.DataFrame:
    """Create empty DataFrame with correct columns"""
    columns = [
        'Normalized_Supplier_Name', 'Original_Name_Variants',
        'Supplier_Description', 'Employee_Count', 'Revenue', 'Year_Established',
        'Overall_Category', 'Category_Code', 'Category_Name', 'Category_Confidence', 'L3_Token_Probability', 'Confidence_Reasoning',
        'Product_Service_Tags', 'Total_PO_Items',
        'Ship_To_Countries', 'Country_Codes', 'Last_Updated'
    ]
    return pd.DataFrame(columns=columns)


def save_genpact_supplier_master(df: pd.DataFrame, file_path: str):
    """Save Genpact Supplier Master to disk"""
    df.to_csv(file_path, index=False, encoding='utf-8')


###########
# Gemini API Functions
###########
def call_gemini_sync(
    model: str,
    api_key: str,
    system_text: str,
    user_text: str,
    temperature: float = 0.2,
    use_grounding: bool = False
) -> Optional[Dict]:
    """Synchronous Gemini API call"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    
    request_body = {
        "contents": [{
            "parts": [{
                "text": f"{system_text}\n\nIMPORTANT: You MUST return ONLY valid JSON, no markdown, no explanations.\n\n{user_text}" if use_grounding else f"{system_text}\n\n{user_text}"
            }]
        }],
        "generationConfig": {
            "temperature": temperature,
            "topP": 0.9,
            "maxOutputTokens": 1024
        }
    }
    
    if not use_grounding:
        request_body["generationConfig"]["responseMimeType"] = "application/json"
    
    if use_grounding:
        if '1.5' in model:
            request_body["tools"] = [{"google_search_retrieval": {"disable_attribution": False}}]
        else:
            request_body["tools"] = [{"googleSearch": {}}]
    
    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json=request_body,
            timeout=60
        )
        
        if not response.ok:
            raise Exception(f"API Error {response.status_code}: {response.text[:500]}")
        
        data = response.json()
        content = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
        
        if not content:
            raise Exception("No content in response")
        
        result = safe_extract_json(content)
        if result is None:
            return {"error": "Failed to parse JSON", "raw": content}
        return result
    
    except Exception as e:
        raise Exception(f"Gemini API error: {str(e)}")


###########
# AI Processing Functions
###########
def enrich_supplier(
    supplier_name: str,
    api_key: str,
    model: str,
    temperature: float,
    use_grounding: bool,
    rate_limiter: RateLimiter
) -> Dict[str, Any]:
    """Enrich supplier with description, employee count, revenue, year established"""
    system_prompt = """You are a business research analyst. Use Google Search to find accurate, current information about companies.
    
Return ONLY valid JSON with these exact fields:
- description: Brief company description (max 50 words)
- employee_count: Number or range (e.g., "10,000-50,000" or "Unknown")
- revenue: Annual revenue with currency (e.g., "$5.2 billion" or "Unknown")
- year_established: Year founded (e.g., "1975" or "Unknown")
- confidence: Your confidence score 0.0-1.0"""
    
    user_prompt = f"""Research this company: "{supplier_name}"
    
Return JSON: {{"description": "...", "employee_count": "...", "revenue": "...", "year_established": "...", "confidence": 0.0}}"""
    
    rate_limiter.wait_if_needed()
    
    try:
        result = call_gemini_sync(
            model=model,
            api_key=api_key,
            system_text=system_prompt,
            user_text=user_prompt,
            temperature=temperature,
            use_grounding=use_grounding
        )
        
        if result and isinstance(result, dict) and not result.get('error'):
            return {
                'description': str(result.get('description', 'Not available') or 'Not available'),
                'employee_count': str(result.get('employee_count', 'Unknown') or 'Unknown'),
                'revenue': str(result.get('revenue', 'Unknown') or 'Unknown'),
                'year_established': str(result.get('year_established', 'Unknown') or 'Unknown'),
                'confidence': float(result.get('confidence', 0.0) or 0.0)
            }
        return {'description': 'Not available', 'employee_count': 'Unknown',
                'revenue': 'Unknown', 'year_established': 'Unknown', 'confidence': 0.0}
    
    except Exception as e:
        return {'description': 'Not available', 'employee_count': 'Unknown',
                'revenue': 'Unknown', 'year_established': 'Unknown', 'confidence': 0.0}


def generate_supplier_product_tags(
    supplier_name: str,
    item_descriptions: List[str],
    api_key: str,
    model: str,
    temperature: float,
    rate_limiter: RateLimiter
) -> List[str]:
    """Generate consolidated product/service tags for a supplier"""
    if not item_descriptions:
        return []
    
    clean_descriptions = list(set([
        str(d).strip() for d in item_descriptions
        if d and str(d).strip() and str(d).strip().lower() != 'nan'
    ]))
    
    if not clean_descriptions:
        return []
    
    max_descriptions = 100
    if len(clean_descriptions) > max_descriptions:
        clean_descriptions = random.sample(clean_descriptions, max_descriptions)
    
    system_prompt = """You are a procurement analyst. Analyze item descriptions to determine what products/services a supplier provides.
    
Generate 3-15 concise tags (1-4 words each) representing their offerings.
Return ONLY valid JSON."""
    
    user_prompt = f"""Supplier: "{supplier_name}"

Item descriptions:
{json.dumps(clean_descriptions[:50], indent=2)}
{"... and " + str(len(clean_descriptions) - 50) + " more" if len(clean_descriptions) > 50 else ""}

Return: {{"product_service_tags": ["tag1", "tag2", ...]}}"""
    
    rate_limiter.wait_if_needed()
    
    try:
        result = call_gemini_sync(
            model=model,
            api_key=api_key,
            system_text=system_prompt,
            user_text=user_prompt,
            temperature=temperature,
            use_grounding=False
        )
        
        if result and isinstance(result, dict) and not result.get('error'):
            tags = result.get('product_service_tags', [])
            return tags if isinstance(tags, list) else []
        return []
    
    except Exception as e:
        return []


def classify_supplier(
    supplier_name: str,
    description: str,
    categories: List[Dict],
    api_key: str,
    model: str,
    temperature: float,
    rate_limiter: RateLimiter
) -> Dict[str, Any]:
    """Classify a supplier into a category using L1/L2/L3 taxonomy"""
    candidates = prefilter_categories(f"{supplier_name} {description}", categories)
    
    system_prompt = """You are a procurement classification expert. You must select from the provided taxonomy paths only.
Each path has three levels: L1 (broad), L2 (mid), L3 (specific).
Return ONLY valid JSON with the exact L1, L2, L3 values from the provided categories."""
    
    user_prompt = f"""Classify this supplier:
Supplier: "{supplier_name}"
Description: "{description}"

Valid taxonomy paths (L1 -> L2 -> L3):
{json.dumps(candidates, indent=2)}

Return: {{"l1": "...", "l2": "...", "l3": "...", "confidence": 0.0, "confidence_reasoning": "...", "l3_token_probability": 0.0}}

You MUST select an exact path from the list above. Do not invent or combine values.
For confidence_reasoning, briefly explain what factors influenced your confidence score.
For l3_token_probability, estimate the probability (0.0 to 1.0) that the L3 category you selected is the correct token/word choice given the context."""
    
    rate_limiter.wait_if_needed()
    
    try:
        result = call_gemini_sync(
            model=model,
            api_key=api_key,
            system_text=system_prompt,
            user_text=user_prompt,
            temperature=temperature,
            use_grounding=False
        )
        
        if result and isinstance(result, dict) and not result.get('error'):
            return {
                'l1': str(result.get('l1', 'Unclassified') or 'Unclassified'),
                'category_code': str(result.get('l2', 'UNCLASSIFIED') or 'UNCLASSIFIED'),
                'category_name': str(result.get('l3', 'Unclassified') or 'Unclassified'),
                'confidence': float(result.get('confidence', 0.0) or 0.0),
                'confidence_reasoning': str(result.get('confidence_reasoning', '') or ''),
                'l3_token_probability': float(result.get('l3_token_probability', 0.0) or 0.0)
            }
        return {'l1': 'Unclassified', 'category_code': 'UNCLASSIFIED', 'category_name': 'Unclassified', 'confidence': 0.0, 'confidence_reasoning': '', 'l3_token_probability': 0.0}
    
    except Exception as e:
        return {'l1': 'Unclassified', 'category_code': 'UNCLASSIFIED', 'category_name': 'Unclassified', 'confidence': 0.0, 'confidence_reasoning': '', 'l3_token_probability': 0.0}


def classify_po_line(
    po_line_description: str,
    supplier_name: str,
    categories: List[Dict],
    api_key: str,
    model: str,
    temperature: float,
    rate_limiter: RateLimiter
) -> Dict[str, Any]:
    """Classify a single PO line item into a category using L1/L2/L3 taxonomy"""
    candidates = prefilter_categories(po_line_description, categories)
    
    system_prompt = """You are a procurement classification expert. You must classify a purchase order line item into the correct taxonomy category.
Each path has three levels: L1 (broad), L2 (mid), L3 (specific).
Return ONLY valid JSON with the exact L1, L2, L3 values from the provided categories.
Select the single best matching category for this specific item."""
    
    user_prompt = f"""Classify this purchase order line item:
Supplier: "{supplier_name}"
Item Description: "{po_line_description}"

Valid taxonomy paths (L1 -> L2 -> L3):
{json.dumps(candidates, indent=2)}

Return: {{"l1": "...", "l2": "...", "l3": "...", "confidence": 0.0, "confidence_reasoning": "...", "l3_token_probability": 0.0}}

You MUST select exactly ONE path from the list above. Do not invent or combine values.
For confidence_reasoning, briefly explain what factors influenced your confidence score.
For l3_token_probability, estimate the probability (0.0 to 1.0) that the L3 category you selected is the correct token/word choice given the context."""
    
    rate_limiter.wait_if_needed()
    
    try:
        result = call_gemini_sync(
            model=model,
            api_key=api_key,
            system_text=system_prompt,
            user_text=user_prompt,
            temperature=temperature,
            use_grounding=False
        )
        
        if result and isinstance(result, dict) and not result.get('error'):
            return {
                'l1': str(result.get('l1', 'Unclassified') or 'Unclassified'),
                'l2': str(result.get('l2', 'UNCLASSIFIED') or 'UNCLASSIFIED'),
                'l3': str(result.get('l3', 'Unclassified') or 'Unclassified'),
                'confidence': float(result.get('confidence', 0.0) or 0.0),
                'confidence_reasoning': str(result.get('confidence_reasoning', '') or ''),
                'l3_token_probability': float(result.get('l3_token_probability', 0.0) or 0.0)
            }
        return {'l1': 'Unclassified', 'l2': 'UNCLASSIFIED', 'l3': 'Unclassified', 'confidence': 0.0, 'confidence_reasoning': '', 'l3_token_probability': 0.0}
    
    except Exception as e:
        return {'l1': 'Unclassified', 'l2': 'UNCLASSIFIED', 'l3': 'Unclassified', 'confidence': 0.0, 'confidence_reasoning': '', 'l3_token_probability': 0.0}


def classify_po_lines_individually(
    po_line_descriptions: List[str],
    supplier_name: str,
    categories: List[Dict],
    api_key: str,
    model: str,
    temperature: float,
    rate_limiter: RateLimiter,
    log_callback=None
) -> List[Dict[str, Any]]:
    """
    Classify each PO line individually.
    Returns a list of classification results with descriptions.
    """
    if not po_line_descriptions:
        return []
    
    results = []
    total = len(po_line_descriptions)
    
    for idx, desc in enumerate(po_line_descriptions):
        if log_callback and idx % 10 == 0:
            log_callback(f"  Classifying PO line {idx+1}/{total}...")
        
        classification = classify_po_line(
            po_line_description=desc,
            supplier_name=supplier_name,
            categories=categories,
            api_key=api_key,
            model=model,
            temperature=temperature,
            rate_limiter=rate_limiter
        )
        results.append({
            'description': desc,
            'l1': classification['l1'],
            'l2': classification['l2'],
            'l3': classification['l3'],
            'confidence': classification['confidence'],
            'confidence_reasoning': classification['confidence_reasoning'],
            'l3_token_probability': classification['l3_token_probability']
        })
    
    return results


def consolidate_classifications(
    classified_lines: List[Dict],
    supplier_name: str,
    api_key: str,
    model: str,
    temperature: float,
    rate_limiter: RateLimiter,
    confidence_threshold: float = 0.6
) -> List[Dict[str, Any]]:
    """
    Review classifications and consolidate low-confidence items into better-fitting categories.
    
    Step 1: Identify high-confidence and low-confidence classifications
    Step 2: Ask Gemini if low-confidence items fit better in existing high-confidence categories
    Step 3: Return consolidated groupings
    
    Rules:
    - Do NOT reduce everything to one category
    - Only merge when there's a genuine fit
    - High-confidence classifications stay as-is
    """
    if not classified_lines:
        return []
    
    # Separate high and low confidence classifications
    high_confidence = [line for line in classified_lines if line['confidence'] >= confidence_threshold]
    low_confidence = [line for line in classified_lines if line['confidence'] < confidence_threshold]

    # If no low confidence items, just return grouped results
    if not low_confidence:
        return _group_classified_lines(classified_lines)
    
    # If no high confidence items to compare against, return as-is
    if not high_confidence:
        return _group_classified_lines(classified_lines)
    
    # Get unique high-confidence categories
    high_conf_categories = {}
    for line in high_confidence:
        key = (line['l1'], line['l2'], line['l3'])
        if key not in high_conf_categories:
            high_conf_categories[key] = {
                'l1': line['l1'],
                'l2': line['l2'],
                'l3': line['l3'],
                'sample_items': [],
                'avg_confidence': 0.0
            }
        high_conf_categories[key]['sample_items'].append(line['description'])
    
    # Calculate average confidence per category
    for key in high_conf_categories:
        items = [l for l in high_confidence if (l['l1'], l['l2'], l['l3']) == key]
        high_conf_categories[key]['avg_confidence'] = sum(l['confidence'] for l in items) / len(items)
        # Limit sample items for prompt
        high_conf_categories[key]['sample_items'] = high_conf_categories[key]['sample_items'][:5]
    
    # Prepare consolidation prompt
    system_prompt = """You are a procurement classification expert reviewing PO line classifications.
    
Some items were classified with low confidence. Your job is to determine if these low-confidence items actually belong in one of the established high-confidence categories.

Rules:
1. Only reassign an item if it genuinely fits better in an existing category
2. Do NOT force items into categories just to reduce the number of categories
3. If an item doesn't fit any existing category well, keep its original classification
4. A supplier realistically provides multiple different products/services - this is normal

Return ONLY valid JSON."""
    
    # Build the prompt
    high_conf_summary = []
    for idx, (key, cat) in enumerate(high_conf_categories.items()):
        high_conf_summary.append({
            'category_id': idx,
            'l1': cat['l1'],
            'l2': cat['l2'],
            'l3': cat['l3'],
            'avg_confidence': f"{cat['avg_confidence']:.0%}",
            'sample_items': cat['sample_items']
        })
    
    low_conf_items = []
    for idx, line in enumerate(low_confidence):
        low_conf_items.append({
            'item_id': idx,
            'description': line['description'],
            'current_l1': line['l1'],
            'current_l2': line['l2'],
            'current_l3': line['l3'],
            'confidence': f"{line['confidence']:.0%}"
        })
    
    user_prompt = f"""Supplier: "{supplier_name}"

HIGH-CONFIDENCE CATEGORIES (established groupings):
{json.dumps(high_conf_summary, indent=2)}

LOW-CONFIDENCE ITEMS (need review):
{json.dumps(low_conf_items[:50], indent=2)}
{f"... and {len(low_conf_items) - 50} more items" if len(low_conf_items) > 50 else ""}

For each low-confidence item, decide:
- Should it be reassigned to one of the high-confidence categories? (specify category_id)
- Or should it keep its current classification? (specify "keep")

Return JSON:
{{
    "reassignments": [
        {{"item_id": 0, "action": "reassign", "to_category_id": 2}},
        {{"item_id": 1, "action": "keep"}},
        ...
    ]
}}

Remember: Only reassign if there's a genuine fit. It's OK to keep items in their original categories."""
    
    rate_limiter.wait_if_needed()
    
    try:
        result = call_gemini_sync(
            model=model,
            api_key=api_key,
            system_text=system_prompt,
            user_text=user_prompt,
            temperature=temperature,
            use_grounding=False
        )
        
        if result and isinstance(result, dict) and 'reassignments' in result:
            # Apply reassignments
            category_keys = list(high_conf_categories.keys())
            
            for reassignment in result['reassignments']:
                if not isinstance(reassignment, dict):
                    continue
                
                item_id = reassignment.get('item_id', -1)
                action = reassignment.get('action', 'keep')
                
                if item_id < 0 or item_id >= len(low_confidence):
                    continue
                
                if action == 'reassign':
                    to_cat_id = reassignment.get('to_category_id', -1)
                    if 0 <= to_cat_id < len(category_keys):
                        new_cat = category_keys[to_cat_id]
                        low_confidence[item_id]['l1'] = new_cat[0]
                        low_confidence[item_id]['l2'] = new_cat[1]
                        low_confidence[item_id]['l3'] = new_cat[2]
                        # Keep original confidence - don't artificially boost it
    
    except Exception as e:
        # On error, just proceed with original classifications
        pass
    
    # Combine and group all classifications
    all_lines = high_confidence + low_confidence
    return _group_classified_lines(all_lines)


def _group_classified_lines(classified_lines: List[Dict]) -> List[Dict[str, Any]]:
    """
    Group classified lines by L1+L2+L3 and return structured category groups.
    """
    groups = defaultdict(lambda: {'po_lines': [], 'confidences': [], 'reasonings': [], 'l3_probs': []})
    
    for line in classified_lines:
        key = (line['l1'], line['l2'], line['l3'])
        groups[key]['po_lines'].append(line['description'])
        groups[key]['confidences'].append(line['confidence'])
        groups[key]['reasonings'].append(line.get('confidence_reasoning', ''))
        groups[key]['l3_probs'].append(line.get('l3_token_probability', 0.0))
    
    result = []
    for (l1, l2, l3), data in groups.items():
        avg_conf = sum(data['confidences']) / len(data['confidences']) if data['confidences'] else 0.0
        avg_l3_prob = sum(data['l3_probs']) / len(data['l3_probs']) if data['l3_probs'] else 0.0
        # Combine unique reasonings, limited to avoid huge strings
        unique_reasonings = list(set([r for r in data['reasonings'] if r]))[:5]
        combined_reasoning = ' | '.join(unique_reasonings) if unique_reasonings else ''
        result.append({
            'l1': l1,
            'l2': l2,
            'l3': l3,
            'confidence': avg_conf,
            'confidence_reasoning': combined_reasoning,
            'l3_token_probability': avg_l3_prob,
            'po_lines': data['po_lines']
        })
    
    return result


###########
# Main Application Class
###########
class SupplierMasterApp:
    """Main Tkinter Application"""
    
    def __init__(self, root):
        self.root = root
        self.root.title(f"Supplier Master Generator v{APP_VERSION}")
        self.root.geometry("1300x850")
        self.root.minsize(1100, 700)
        
        # Data storage
        self.po_data = None
        self.client_sm_data = None
        self.categories_data = None
        self.results_data = None
        self.po_file_path = ""
        
        # Column mappings
        self.po_columns = {}
        self.client_sm_columns = {}
        self.category_columns = {}
        
        # Stats
        self.stats = {
            'new_suppliers': 0,
            'updated_suppliers': 0,
            'unchanged_suppliers': 0,
            'total_in_master': 0
        }
        
        # Processing state
        self.processing = False
        self.message_queue = queue.Queue()
        
        # Variables for form inputs
        self.api_key_var = tk.StringVar()
        self.model_var = tk.StringVar(value="gemini-2.0-flash")
        self.rpm_var = tk.IntVar(value=30)
        self.temp_var = tk.DoubleVar(value=0.2)
        self.grounding_var = tk.BooleanVar(value=True)
        
        # Setup UI
        self.setup_ui()
        
        # Start message queue processor
        self.process_queue()
    
    def setup_ui(self):
        """Setup the main UI"""
        # Create main paned window (horizontal split)
        self.main_paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        self.main_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Sidebar (Settings)
        self.sidebar = self.create_sidebar()
        self.main_paned.add(self.sidebar, weight=0)
        
        # Main content area
        self.main_content = self.create_main_content()
        self.main_paned.add(self.main_content, weight=1)
    
    def create_sidebar(self) -> ttk.Frame:
        """Create the sidebar with settings"""
        sidebar = ttk.Frame(self.main_paned, width=300)
        sidebar.pack_propagate(False)
        
        # Header
        header = ttk.Label(sidebar, text="Settings", font=('Helvetica', 14, 'bold'))
        header.pack(pady=(10, 15), padx=10, anchor='w')
        
        # API Settings Frame
        api_frame = ttk.LabelFrame(sidebar, text="API Configuration", padding=10)
        api_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # API Key
        ttk.Label(api_frame, text="API Key:").pack(anchor='w')
        self.api_key_entry = ttk.Entry(api_frame, textvariable=self.api_key_var, show='*', width=30)
        self.api_key_entry.pack(fill=tk.X, pady=(0, 10))
        
        # Model
        ttk.Label(api_frame, text="Model:").pack(anchor='w')
        model_combo = ttk.Combobox(api_frame, textvariable=self.model_var,
                                  values=["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-pro"],
                                  state='readonly', width=28)
        model_combo.pack(fill=tk.X, pady=(0, 10))
        
        # RPM and Temperature in a row
        rpm_temp_frame = ttk.Frame(api_frame)
        rpm_temp_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(rpm_temp_frame, text="RPM:").pack(side=tk.LEFT)
        rpm_spin = ttk.Spinbox(rpm_temp_frame, from_=1, to=100, textvariable=self.rpm_var, width=5)
        rpm_spin.pack(side=tk.LEFT, padx=(5, 15))
        
        ttk.Label(rpm_temp_frame, text="Temp:").pack(side=tk.LEFT)
        temp_spin = ttk.Spinbox(rpm_temp_frame, from_=0.0, to=1.0, increment=0.1,
                               textvariable=self.temp_var, width=5, format="%.1f")
        temp_spin.pack(side=tk.LEFT, padx=5)
        
        # Grounding checkbox
        grounding_check = ttk.Checkbutton(api_frame, text="Enable Google Search Grounding",
                                         variable=self.grounding_var)
        grounding_check.pack(anchor='w')
        
        # Test API Button
        self.test_btn = ttk.Button(sidebar, text="Test API Connection", command=self.test_api)
        self.test_btn.pack(fill=tk.X, padx=10, pady=10)
        
        # Output File Frame
        output_frame = ttk.LabelFrame(sidebar, text="Output File", padding=10)
        output_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.output_path_label = ttk.Label(output_frame, text="Auto-save to input directory",
                                          wraplength=250)
        self.output_path_label.pack(anchor='w')
        
        self.output_status_label = ttk.Label(output_frame, text="No output file yet",
                                            foreground='gray')
        self.output_status_label.pack(anchor='w', pady=(5, 0))
        
        self.delete_btn = ttk.Button(output_frame, text="Reset/Delete Output File",
                                    command=self.delete_output_file, state='disabled')
        self.delete_btn.pack(fill=tk.X, pady=(10, 0))
        
        # Spacer
        ttk.Frame(sidebar).pack(fill=tk.BOTH, expand=True)
        
        # Version info
        version_label = ttk.Label(sidebar, text=f"Build: {APP_BUILD}\nPO line classification flow",
                                 foreground='gray', font=('Helvetica', 9))
        version_label.pack(pady=10, padx=10, anchor='w')
        
        return sidebar
    
    def create_main_content(self) -> ttk.Frame:
        """Create the main content area with tabs"""
        main_frame = ttk.Frame(self.main_paned)
        
        # Header
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        title_label = ttk.Label(header_frame, text="Supplier Master Generator",
                               font=('Helvetica', 18, 'bold'))
        title_label.pack(side=tk.LEFT)
        
        # Version badge (using a label with background)
        version_frame = tk.Frame(header_frame, bg='#27ae60', padx=8, pady=2)
        version_frame.pack(side=tk.LEFT, padx=10)
        tk.Label(version_frame, text=f"v{APP_VERSION}", bg='#27ae60', fg='white',
                font=('Helvetica', 10)).pack()
        
        # Description
        desc_label = ttk.Label(main_frame,
                              text="AI-powered Genpact Supplier Master maintenance with automatic incremental updates.",
                              foreground='gray')
        desc_label.pack(anchor='w', padx=10, pady=(0, 10))
        
        # Feature highlight box
        feature_frame = tk.Frame(main_frame, bg='#e8f8f5', padx=15, pady=10)
        feature_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        # Left border effect
        border = tk.Frame(feature_frame, bg='#27ae60', width=4)
        border.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        
        feature_text = tk.Label(feature_frame, bg='#e8f8f5', justify=tk.LEFT,
                               text="Inputs: PO File + Taxonomy (L1/L2/L3) + Client Supplier Master (optional)\n"
                                    "Output: Automatically updates GenpactSupplierMaster.csv (adds new, updates existing)")
        feature_text.pack(side=tk.LEFT, anchor='w')
        
        # Tab notebook
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Create tabs
        self.upload_tab = self.create_upload_tab()
        self.notebook.add(self.upload_tab, text="📁 Upload Inputs")
        
        self.mapping_tab = self.create_mapping_tab()
        self.notebook.add(self.mapping_tab, text="🔧 Column Mapping")

        self.process_tab = self.create_process_tab()
        self.notebook.add(self.process_tab, text="⚙️ Process")

        self.results_tab = self.create_results_tab()
        self.notebook.add(self.results_tab, text="📊 Results")

        return main_frame

    def create_upload_tab(self) -> ttk.Frame:
        """Create the file upload tab"""
        tab = ttk.Frame(self.notebook, padding=10)

        # Header
        ttk.Label(tab, text="Upload Input Files", font=('Helvetica', 14, 'bold')).pack(anchor='w', pady=(0, 10))

        # Files frame (3 columns) - use pack with side=LEFT for horizontal layout
        files_frame = ttk.Frame(tab)
        files_frame.pack(fill=tk.X, expand=False, pady=(0, 10))

        # PO File
        po_frame = ttk.LabelFrame(files_frame, text="1️⃣ PO File", padding=10)
        po_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))

        ttk.Label(po_frame, text="Supplier Number, Supplier Name, Item Description",
                 foreground='gray', font=('Helvetica', 9)).pack(anchor='w')

        ttk.Button(po_frame, text="Browse PO CSV...",
                  command=lambda: self.load_file('po')).pack(fill=tk.X, pady=(10, 5))

        self.po_status_label = ttk.Label(po_frame, text="No file loaded", foreground='gray')
        self.po_status_label.pack(anchor='w')

        # PO Preview - fixed height
        self.po_preview_tree = self.create_preview_tree(po_frame)

        # Client SM File
        csm_frame = ttk.LabelFrame(files_frame, text="2️⃣ Client Supplier Master (Optional)", padding=10)
        csm_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)

        ttk.Label(csm_frame, text="Supplier Number, Country",
                 foreground='gray', font=('Helvetica', 9)).pack(anchor='w')

        ttk.Button(csm_frame, text="Browse Client SM CSV...",
                  command=lambda: self.load_file('csm')).pack(fill=tk.X, pady=(10, 5))

        self.csm_status_label = ttk.Label(csm_frame, text="No file loaded", foreground='gray')
        self.csm_status_label.pack(anchor='w')

        # CSM Preview - fixed height
        self.csm_preview_tree = self.create_preview_tree(csm_frame)

        # Categories File
        cat_frame = ttk.LabelFrame(files_frame, text="3️⃣ Taxonomy", padding=10)
        cat_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))

        ttk.Label(cat_frame, text="Genpact Level 1, Level 2, Level 3",
                 foreground='gray', font=('Helvetica', 9)).pack(anchor='w')

        ttk.Button(cat_frame, text="Browse Categories CSV...",
                  command=lambda: self.load_file('cat')).pack(fill=tk.X, pady=(10, 5))

        self.cat_status_label = ttk.Label(cat_frame, text="No file loaded", foreground='gray')
        self.cat_status_label.pack(anchor='w')

        # Taxonomy Preview - fixed height
        self.cat_preview_tree = self.create_preview_tree(cat_frame)

        # Output info box
        output_frame = tk.Frame(tab, bg='#fef9e7', padx=15, pady=10)
        output_frame.pack(fill=tk.X, pady=(10, 0), side=tk.BOTTOM)

        border = tk.Frame(output_frame, bg='#f39c12', width=4)
        border.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))

        self.output_info_label = tk.Label(output_frame, bg='#fef9e7',
                                         text="Output: Results will be saved to the same directory as the PO file")
        self.output_info_label.pack(side=tk.LEFT, anchor='w')

        return tab

    def create_preview_tree(self, parent) -> ttk.Treeview:
        """Create a preview treeview for CSV data - with FIXED height"""
        tree_frame = ttk.Frame(parent, height=120)
        tree_frame.pack(fill=tk.X, expand=False, pady=(10, 0))
        tree_frame.pack_propagate(False)  # Prevent frame from shrinking/expanding

        # Treeview with fixed height of 4 rows
        tree = ttk.Treeview(tree_frame, height=4, show='headings')

        # Horizontal scrollbar
        x_scroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=tree.xview)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        # Vertical scrollbar
        y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        return tree

    def create_mapping_tab(self) -> ttk.Frame:
        """Create the column mapping tab"""
        tab = ttk.Frame(self.notebook, padding=10)

        ttk.Label(tab, text="Column Mapping", font=('Helvetica', 14, 'bold')).pack(anchor='w', pady=(0, 15))

        # Mapping frames
        mapping_frame = ttk.Frame(tab)
        mapping_frame.pack(fill=tk.X)
        mapping_frame.columnconfigure(0, weight=1)
        mapping_frame.columnconfigure(1, weight=1)
        mapping_frame.columnconfigure(2, weight=1)

        # PO Mapping
        po_map_frame = ttk.LabelFrame(mapping_frame, text="PO File Columns", padding=10)
        po_map_frame.grid(row=0, column=0, sticky='nsew', padx=5, pady=5)

        ttk.Label(po_map_frame, text="Supplier Number:").pack(anchor='w')
        self.po_num_combo = ttk.Combobox(po_map_frame, state='readonly', width=25)
        self.po_num_combo.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(po_map_frame, text="Supplier Name:").pack(anchor='w')
        self.po_name_combo = ttk.Combobox(po_map_frame, state='readonly', width=25)
        self.po_name_combo.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(po_map_frame, text="Item Description:").pack(anchor='w')
        self.po_item_combo = ttk.Combobox(po_map_frame, state='readonly', width=25)
        self.po_item_combo.pack(fill=tk.X)

        # CSM Mapping
        csm_map_frame = ttk.LabelFrame(mapping_frame, text="Client Supplier Master Columns", padding=10)
        csm_map_frame.grid(row=0, column=1, sticky='nsew', padx=5, pady=5)

        ttk.Label(csm_map_frame, text="Supplier Number:").pack(anchor='w')
        self.csm_num_combo = ttk.Combobox(csm_map_frame, state='readonly', width=25)
        self.csm_num_combo.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(csm_map_frame, text="Country:").pack(anchor='w')
        self.csm_country_combo = ttk.Combobox(csm_map_frame, state='readonly', width=25)
        self.csm_country_combo.pack(fill=tk.X)

        # Category Mapping - Updated for L1/L2/L3
        cat_map_frame = ttk.LabelFrame(mapping_frame, text="Taxonomy Columns", padding=10)
        cat_map_frame.grid(row=0, column=2, sticky='nsew', padx=5, pady=5)

        ttk.Label(cat_map_frame, text="Genpact Level 1:").pack(anchor='w')
        self.cat_l1_combo = ttk.Combobox(cat_map_frame, state='readonly', width=25)
        self.cat_l1_combo.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(cat_map_frame, text="Genpact Level 2 (Category Code):").pack(anchor='w')
        self.cat_l2_combo = ttk.Combobox(cat_map_frame, state='readonly', width=25)
        self.cat_l2_combo.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(cat_map_frame, text="Genpact Level 3 (Category Name):").pack(anchor='w')
        self.cat_l3_combo = ttk.Combobox(cat_map_frame, state='readonly', width=25)
        self.cat_l3_combo.pack(fill=tk.X)

        return tab

    def create_process_tab(self) -> ttk.Frame:
    """Create the processing tab"""
    tab = ttk.Frame(self.notebook, padding=10)

    ttk.Label(tab, text="Run Processing", font=('Helvetica', 14, 'bold')).pack(anchor='w', pady=(0, 10))

    # Readiness status
    self.readiness_label = ttk.Label(tab, text="Checking readiness...", foreground='orange')
    self.readiness_label.pack(anchor='w', pady=(0, 10))

    # Process button
    self.process_btn = ttk.Button(tab, text="▶ Start Processing", command=self.start_processing)
    self.process_btn.pack(fill=tk.X, pady=(0, 15), ipady=10)

    # Progress frame
    progress_frame = ttk.LabelFrame(tab, text="Progress", padding=10)
    progress_frame.pack(fill=tk.X, pady=(0, 10))

    self.progress_var = tk.DoubleVar(value=0)
    self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, maximum=100)
    self.progress_bar.pack(fill=tk.X, pady=(0, 5))

    self.status_label = ttk.Label(progress_frame, text="Ready")
    self.status_label.pack(anchor='w')

    # Log frame
    log_frame = ttk.LabelFrame(tab, text="Processing Log", padding=10)
    log_frame.pack(fill=tk.BOTH, expand=True)

    self.log_text = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD,
                                             bg='#2c3e50', fg='#ecf0f1',
                                             font=('Consolas', 10))
    self.log_text.pack(fill=tk.BOTH, expand=True)

    # Configure log tags for colors
    self.log_text.tag_configure('success', foreground='#27ae60')
    self.log_text.tag_configure('error', foreground='#e74c3c')
    self.log_text.tag_configure('warning', foreground='#f39c12')
    self.log_text.tag_configure('grounding', foreground='#3498db')
    self.log_text.tag_configure('update', foreground='#9b59b6')
    self.log_text.tag_configure('info', foreground='#ecf0f1')

    return tab

def create_results_tab(self) -> ttk.Frame:
    """Create the results tab"""
    tab = ttk.Frame(self.notebook, padding=10)

    ttk.Label(tab, text="Genpact Supplier Master", font=('Helvetica', 14, 'bold')).pack(anchor='w', pady=(0, 10))

    # Stats frame
    stats_frame = ttk.Frame(tab)
    stats_frame.pack(fill=tk.X, pady=(0, 15))

    self.stat_labels = {}
    for i, (key, label) in enumerate([('new', 'New'), ('updated', 'Updated'),
                                      ('unchanged', 'Unchanged'), ('total', 'Total')]):
        stat_box = ttk.Frame(stats_frame, relief='solid', borderwidth=1)
        stat_box.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        value_label = ttk.Label(stat_box, text="0", font=('Helvetica', 24, 'bold'))
        value_label.pack(pady=(10, 0))

        name_label = ttk.Label(stat_box, text=label, foreground='gray')
        name_label.pack(pady=(0, 10))

        self.stat_labels[key] = value_label

    # Results treeview
    tree_frame = ttk.Frame(tab)
    tree_frame.pack(fill=tk.BOTH, expand=True)

    # Create treeview with scrollbars
    self.results_tree = ttk.Treeview(tree_frame, show='headings')

    y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
    x_scroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.results_tree.xview)
    self.results_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

    self.results_tree.grid(row=0, column=0, sticky='nsew')
    y_scroll.grid(row=0, column=1, sticky='ns')
    x_scroll.grid(row=1, column=0, sticky='ew')

    tree_frame.columnconfigure(0, weight=1)
    tree_frame.rowconfigure(0, weight=1)

    # Export buttons
    export_frame = ttk.Frame(tab)
    export_frame.pack(fill=tk.X, pady=(10, 0))

    self.export_btn = ttk.Button(export_frame, text="💾 Export CSV", command=self.export_results,
                                state='disabled')
    self.export_btn.pack(side=tk.LEFT)

    self.saved_path_label = ttk.Label(export_frame, text="", foreground='green')
    self.saved_path_label.pack(side=tk.LEFT, padx=10)

    return tab

def find_col_index(self, columns: list, keywords: list) -> int:
    """Find column index matching keywords"""
    for i, c in enumerate(columns):
        if any(k in c.lower() for k in keywords):
            return i
    return 0

def update_preview_tree(self, tree: ttk.Treeview, df: pd.DataFrame):
    """Update a preview treeview with dataframe data"""
    # Clear existing
    tree.delete(*tree.get_children())
    for col in tree['columns']:
        tree.heading(col, text='')

    # Set columns
    columns = list(df.columns)
    tree['columns'] = columns

    for col in columns:
        tree.heading(col, text=col)
        tree.column(col, width=100, minwidth=50)

    # Add rows (first 3)
    for idx, row in df.head(3).iterrows():
        values = [str(v)[:50] for v in row.values]
        tree.insert('', tk.END, values=values)

def load_file(self, file_type: str):
    """Load a CSV file"""
    file_path = filedialog.askopenfilename(
        title=f"Open {file_type.upper()} CSV",
        filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
    )

    if not file_path:
        return

    try:
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except:
            df = pd.read_csv(file_path, encoding='latin-1')

        columns = df.columns.tolist()

        if file_type == 'po':
            self.po_data = df
            self.po_file_path = file_path
            self.po_status_label.configure(text=f"✓ {len(df)} rows loaded", foreground='green')
            self.update_preview_tree(self.po_preview_tree, df)

            # Update column combos
            self.po_num_combo['values'] = columns
            self.po_num_combo.current(self.find_col_index(columns,
                                 ['supplier_number', 'supplier_num', 'vendor_number', 'vendor_id']))

            self.po_name_combo['values'] = columns
            self.po_name_combo.current(self.find_col_index(columns,
                                  ['supplier_name', 'vendor_name', 'supplier']))

            self.po_item_combo['values'] = columns
            self.po_item_combo.current(self.find_col_index(columns,
                                  ['item_desc', 'description', 'item']))

            # Update output path
            output_dir = os.path.dirname(file_path)
            output_file = os.path.join(output_dir, "GenpactSupplierMaster.csv")
            self.output_info_label.configure(text=f"Output: {output_file}")
            self.update_output_status(output_file)

        elif file_type == 'csm':
            self.client_sm_data = df
            self.csm_status_label.configure(text=f"✓ {len(df)} rows loaded", foreground='green')
            self.update_preview_tree(self.csm_preview_tree, df)

            self.csm_num_combo['values'] = columns
            self.csm_num_combo.current(self.find_col_index(columns,
                ['supplier_number', 'supplier_num', 'vendor_number']))

            self.csm_country_combo['values'] = columns
            self.csm_country_combo.current(self.find_col_index(columns,
                ['country', 'country_code']))

        elif file_type == 'cat':
            self.categories_data = df
            self.cat_status_label.configure(text=f"✓ {len(df)} categories loaded", foreground='green')
            self.update_preview_tree(self.cat_preview_tree, df)

            # Updated for L1/L2/L3 taxonomy
            self.cat_l1_combo['values'] = columns
            self.cat_l1_combo.current(self.find_col_index(columns,
                ['genpact level 1', 'level 1', 'l1']))

            self.cat_l2_combo['values'] = columns
            self.cat_l2_combo.current(self.find_col_index(columns,
                ['genpact level 2', 'level 2', 'l2', 'category_code', 'code']))

            self.cat_l3_combo['values'] = columns
            self.cat_l3_combo.current(self.find_col_index(columns,
                ['genpact level 3', 'level 3', 'l3', 'category_name', 'name']))

            self.update_readiness()

    except Exception as e:
        messagebox.showerror("Error", f"Failed to load file:\n{str(e)}")

def update_output_status(self, file_path: str):
    """Update output file status"""
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        self.output_status_label.configure(text=f"✓ File exists ({file_size} bytes)", foreground='green')
        self.delete_btn.configure(state='normal')
        self.output_path_label.configure(text=file_path)
    else:
        self.output_status_label.configure(text="File will be created", foreground='gray')
        self.delete_btn.configure(state='disabled')
        self.output_path_label.configure(text=file_path)

def delete_output_file(self):
    """Delete the output file"""
    if not self.po_file_path:
        return

    output_file = os.path.join(os.path.dirname(self.po_file_path), "GenpactSupplierMaster.csv")

    if os.path.exists(output_file):
        if messagebox.askyesno("Confirm Delete", f"Delete {output_file}?"):
            try:
                os.remove(output_file)
                self.update_output_status(output_file)
                messagebox.showinfo("Success", "File deleted. Will create new on next run.")
            except Exception as e:
                messagebox.showerror("Error", f"Could not delete: {e}")

def update_readiness(self):
    """Update the readiness status"""
    missing = []

    if not self.api_key_var.get():
        missing.append("API Key")
    if self.po_data is None:
        missing.append("PO File")
    # Client SM is optional - only used for country lookup
    if self.categories_data is None:
        missing.append("Taxonomy")

    if missing:
        self.readiness_label.configure(text=f"❌ Missing: {', '.join(missing)}", foreground='red')
        self.process_btn.configure(state='disabled')
    else:
        self.readiness_label.configure(text="✓ Ready to process", foreground='green')
        self.process_btn.configure(state='normal' if not self.processing else 'disabled')

def test_api(self):
    """Test the API connection"""
    api_key = self.api_key_var.get()
    if not api_key:
        messagebox.showwarning("Warning", "Please enter an API key")
        return

    self.test_btn.configure(state='disabled', text="Testing...")
    self.root.update()

    try:
        model = self.model_var.get()
        result = call_gemini_sync(model, api_key, "Test.", 'Return: {"status": "ok"}', 0.2, False)

        if result and result.get('status') == 'ok':
            messagebox.showinfo("Success", "API connection successful!")
        else:
            messagebox.showwarning("Warning", f"API responded but unexpected result: {result}")
    except Exception as e:
        messagebox.showerror("Error", f"API test failed:\n{str(e)[:200]}")
    finally:
        self.test_btn.configure(state='normal', text="Test API Connection")
        self.update_readiness()

def log_message(self, message: str, msg_type: str = 'info'):
    """Add a log message"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    self.log_text.insert(tk.END, f"[{timestamp}] {message}\n", msg_type)
    self.log_text.see(tk.END)

def process_queue(self):
    """Process messages from the worker thread"""
    try:
        while True:
            msg = self.message_queue.get_nowait()
            msg_type = msg.get('type')

            if msg_type == 'progress':
                self.progress_var.set(msg['value'] * 100)
                self.status_label.configure(text=msg['status'])
            elif msg_type == 'log':
                self.log_message(msg['message'], msg['level'])
            elif msg_type == 'complete':
                self.on_processing_complete(msg['results'], msg['stats'])
            elif msg_type == 'error':
                self.on_processing_error(msg['error'])

    except queue.Empty:
        pass

    # Schedule next check
    self.root.after(100, self.process_queue)

def start_processing(self):
    """Start the processing in a background thread"""
    if self.processing:
        return

    # Collect column mappings
    self.po_columns = {
        'supplier_number': self.po_num_combo.get(),
        'supplier_name': self.po_name_combo.get(),
        'item_description': self.po_item_combo.get()
    }

    self.client_sm_columns = {
        'supplier_number': self.csm_num_combo.get(),
        'country': self.csm_country_combo.get()
    }

    # Updated for L1/L2/L3 taxonomy
    self.category_columns = {
        'l1': self.cat_l1_combo.get(),
        'l2': self.cat_l2_combo.get(),
        'l3': self.cat_l3_combo.get()
    }

    # Clear log
    self.log_text.delete(1.0, tk.END)
    self.progress_var.set(0)
    self.status_label.configure(text="Starting...")

    # Start processing thread
    self.processing = True
    self.process_btn.configure(state='disabled', text="Processing...")

    thread = threading.Thread(target=self.run_processing, daemon=True)
    thread.start()

def run_processing(self):
    """Run the processing in background thread"""
    try:
        output_dir = os.path.dirname(self.po_file_path)
        genpact_sm_path = os.path.join(output_dir, "GenpactSupplierMaster.csv")

        results_df, stats = self.process_all_data(
            genpact_sm_path=genpact_sm_path,
            api_key=self.api_key_var.get(),
            model=self.model_var.get(),
            temperature=self.temp_var.get(),
            use_grounding=self.grounding_var.get(),
            rpm_limit=self.rpm_var.get()
        )

        self.message_queue.put({
            'type': 'complete',
            'results': results_df,
            'stats': stats
        })

    except Exception as e:
        self.message_queue.put({
            'type': 'error',
            'error': str(e)
        })

def emit_progress(self, progress: float, status: str):
    """Emit progress update to main thread"""
    self.message_queue.put({
        'type': 'progress',
        'value': progress,
        'status': status
    })

def emit_log(self, message: str, level: str = 'info'):
    """Emit log message to main thread"""
    self.message_queue.put({
        'type': 'log',
        'message': message,
        'level': level
    })

def normalize_supplier_names(self, supplier_names: List[str], rate_limiter: RateLimiter) -> Dict[str, str]:
    """Normalize supplier names using hybrid approach"""
    if not supplier_names:
        return {}

    unique_names = list(set([str(n).strip() for n in supplier_names if n and str(n).strip()]))

    if len(unique_names) == 0:
        return {}

    if len(unique_names) == 1:
        return {unique_names[0]: unique_names[0]}

    self.emit_log(f"Clustering {len(unique_names)} unique supplier names...", 'info')
    self.emit_progress(0.1, "Pre-clustering supplier names algorithmically...")

    clusters = cluster_suppliers_algorithmic(unique_names, threshold=0.65)

    confirmed_clusters = []
    ambiguous_clusters = []
    singleton_names = []

    for cluster in clusters:
        if len(cluster) == 1:
            singleton_names.append(cluster[0])
        elif len(cluster) > 1:
            scores = []
            for i, n1 in enumerate(cluster):
                for n2 in cluster[i+1:]:
                    scores.append(company_similarity(n1, n2))
            avg_score = sum(scores) / len(scores) if scores else 0

            if avg_score >= 0.85:
                confirmed_clusters.append(cluster)
            else:
                ambiguous_clusters.append(cluster)

    self.emit_log(f"Pre-clustering: {len(confirmed_clusters)} confirmed, {len(ambiguous_clusters)} ambiguous, {len(singleton_names)} singletons", 'info')

    for cluster in confirmed_clusters[:5]:
        self.emit_log(f"Auto-grouped: {cluster}", 'success')

    name_map = {}

    for cluster in confirmed_clusters:
        canonical = pick_canonical_name(cluster)
        for name in cluster:
            name_map[name] = canonical
        if len(cluster) > 1:
            self.emit_log(f"Canonical selected: '{canonical}' from {cluster}", 'success')

    for name in singleton_names:
        name_map[name] = name

    # Process ambiguous clusters with LLM
    if ambiguous_clusters:
        self.emit_progress(0.4, f"LLM confirming {len(ambiguous_clusters)} ambiguous clusters...")
        self.emit_log(f"Sending {len(ambiguous_clusters)} ambiguous clusters to LLM for confirmation...", 'info')

        system_prompt = """You are a supplier data expert. For each cluster of company names, determine:
1. Are these names referring to the SAME company? (Yes/No)
2. If Yes, what is the best canonical name?

Return ONLY valid JSON."""

        batch_size = 10
        for batch_idx in range(0, len(ambiguous_clusters), batch_size):
            batch = ambiguous_clusters[batch_idx:batch_idx + batch_size]

            clusters_for_llm = [{"cluster_id": idx, "names": cluster} for idx, cluster in enumerate(batch)]

            user_prompt = f"""Analyze these {len(batch)} clusters:

{json.dumps(clusters_for_llm, indent=2)}

Return JSON:
{{
    "results": [
        {{"cluster_id": 0, "same_company": true, "canonical_name": "Best Name Inc"}},
        {{"cluster_id": 1, "same_company": false, "reason": "Different companies"}}
    ]
}}"""

                rate_limiter.wait_if_needed()

                try:
                    result = call_gemini_sync(
                        model=self.model_var.get(),
                        api_key=self.api_key_var.get(),
                        system_text=system_prompt,
                        user_text=user_prompt,
                        temperature=self.temp_var.get(),
                        use_grounding=False
                    )

                    if result and isinstance(result, dict) and 'results' in result:
                        for item in result['results']:
                            if not isinstance(item, dict):
                                continue
                            cluster_id = item.get('cluster_id', -1)
                            if cluster_id < 0 or cluster_id >= len(batch):
                                continue

                            cluster = batch[cluster_id]

                            if item.get('same_company', False):
                                canonical = pick_canonical_name(cluster)
                                for name in cluster:
                                    name_map[name] = canonical
                                self.emit_log(f"LLM confirmed: {cluster} -> {canonical}", 'success')
                            else:
                                for name in cluster:
                                    name_map[name] = name
                                self.emit_log(f"↔ LLM separated: {cluster}", 'info')
                    else:
                        for cluster in batch:
                            canonical = pick_canonical_name(cluster)
                            for name in cluster:
                                name_map[name] = canonical

                except Exception as e:
                    self.emit_log(f"LLM confirmation error: {str(e)}", 'warning')
                    for cluster in batch:
                        canonical = pick_canonical_name(cluster)
                        for name in cluster:
                            name_map[name] = canonical

                progress = 0.4 + ((batch_idx + batch_size) / len(ambiguous_clusters)) * 0.5
                self.emit_progress(min(progress, 0.9), f"LLM confirming clusters... ({min(batch_idx + batch_size, len(ambiguous_clusters))}/{len(ambiguous_clusters)})")

        for name in unique_names:
            if name not in name_map:
                name_map[name] = name

        unique_canonical = set(name_map.values())
        dedup_count = len(unique_names) - len(unique_canonical)

        if dedup_count > 0:
            self.emit_log(f"Deduplicated {len(unique_names)} names -> {len(unique_canonical)} unique suppliers ({dedup_count} duplicates merged)", 'success')
        else:
            self.emit_log(f"All {len(unique_names)} supplier names are unique", 'info')

        return name_map

    def process_all_data(self, genpact_sm_path: str, api_key: str, model: str,
                        temperature: float, use_grounding: bool, rpm_limit: int) -> Tuple[pd.DataFrame, dict]:
        """Main processing orchestrator"""
        rate_limiter = RateLimiter(max_rpm=rpm_limit)
        stats = {'new_suppliers': 0, 'updated_suppliers': 0, 'unchanged_suppliers': 0, 'total_in_master': 0}

        po_supplier_num_col = self.po_columns.get('supplier_number')
        po_supplier_name_col = self.po_columns.get('supplier_name')
        po_item_col = self.po_columns.get('item_description')

        csm_supplier_num_col = self.client_sm_columns.get('supplier_number')
        csm_country_col = self.client_sm_columns.get('country')

        # Updated for L1/L2/L3 taxonomy
        cat_l1_col = self.category_columns.get('l1')
        cat_l2_col = self.category_columns.get('l2')
        cat_l3_col = self.category_columns.get('l3')

        # STEP 1: Build Country lookup (optional - only if Client SM provided)
        self.emit_log("Step 1: Building country lookup from Client Supplier Master...", 'info')
        self.emit_progress(0.05, "Building country lookup...")

        country_lookup = {}
        if self.client_sm_data is not None and csm_supplier_num_col and csm_country_col:
            for _, row in self.client_sm_data.iterrows():
                supplier_num = str(row[csm_supplier_num_col]).strip() if csm_supplier_num_col else None
                if supplier_num and supplier_num.lower() != 'nan':
                    country_val = str(row[csm_country_col]) if csm_country_col else ''
                    if country_val and country_val.lower() != 'nan':
                        country_lookup[supplier_num] = country_val
            self.emit_log(f"Built country lookup for {len(country_lookup)} suppliers", 'info')
        else:
            self.emit_log("No Client Supplier Master provided - country data will be 'Unknown'", 'info')

        # STEP 2: Extract supplier data from PO file
        self.emit_log("Step 2: Extracting suppliers from PO file...", 'info')

        po_suppliers = defaultdict(lambda: {'items': [], 'countries': set(), 'original_names': set()})

        for _, row in self.po_data.iterrows():
            supplier_num = str(row[po_supplier_num_col]).strip() if po_supplier_num_col else ''
            supplier_name = str(row[po_supplier_name_col]).strip() if po_supplier_name_col else ''
            item_desc = str(row[po_item_col]) if po_item_col else ''

            if not supplier_name or supplier_name.lower() == 'nan':
                continue

            po_suppliers[supplier_name]['original_names'].add(supplier_name)

            if item_desc and item_desc.lower() != 'nan':
                po_suppliers[supplier_name]['items'].append(item_desc)

            if supplier_num and supplier_num in country_lookup:
                po_suppliers[supplier_name]['countries'].add(country_lookup[supplier_num])

        self.emit_log(f"Found {len(po_suppliers)} unique supplier names in PO file", 'info')

        # STEP 3: Normalize supplier names
        self.emit_log("Step 3: Normalizing supplier names...", 'info')
        self.emit_progress(0.10, "Normalizing supplier names...")

        all_supplier_names = list(po_suppliers.keys())
        name_normalization_map = self.normalize_supplier_names(all_supplier_names, rate_limiter)

        self.emit_log(f"Normalized {len(name_normalization_map)} supplier names", 'success')

        # Regroup by normalized name
        normalized_suppliers = defaultdict(lambda: {'items': [], 'countries': set(), 'original_names': set()})

        for original_name, data in po_suppliers.items():
            normalized_name = name_normalization_map.get(original_name, original_name)
            if not normalized_name or not str(normalized_name).strip():
                normalized_name = original_name
            if not normalized_name or not str(normalized_name).strip():
                continue
            normalized_suppliers[normalized_name]['items'].extend(data['items'])
            normalized_suppliers[normalized_name]['countries'].update(data['countries'])
            normalized_suppliers[normalized_name]['original_names'].add(original_name)

        self.emit_log(f"After normalization: {len(normalized_suppliers)} unique suppliers", 'info')

        # STEP 4: Load existing Genpact Supplier Master
        self.emit_log(f"Step 4: Loading Genpact Supplier Master from {genpact_sm_path}...", 'info')
        self.emit_progress(0.25, "Loading Genpact Supplier Master...")

        genpact_sm_df = load_genpact_supplier_master(genpact_sm_path)

        existing_suppliers = {}
        if len(genpact_sm_df) > 0:
            for idx, row in genpact_sm_df.iterrows():
                norm_name = str(row.get('Normalized_Supplier_Name', '')).strip()
                if norm_name:
                    existing_suppliers[norm_name.lower()] = row.to_dict()
            self.emit_log(f"Loaded {len(existing_suppliers)} existing suppliers", 'info')
        else:
            self.emit_log("No existing Genpact Supplier Master - will create new", 'info')

        # STEP 5: Identify new vs existing
        self.emit_log("Step 5: Identifying new vs existing suppliers...", 'info')

        new_suppliers = []
        existing_to_update = []

        for norm_name in normalized_suppliers.keys():
            if not norm_name or not str(norm_name).strip():
                continue
            if str(norm_name).lower() in existing_suppliers:
                existing_to_update.append(norm_name)
            else:
                new_suppliers.append(norm_name)

        self.emit_log(f"New suppliers to add: {len(new_suppliers)}", 'info')
        self.emit_log(f"Existing suppliers to update: {len(existing_to_update)}", 'update')

        # Build categories list - Updated for L1/L2/L3
        categories = []
        for _, row in self.categories_data.iterrows():
            categories.append({
                'l1': str(row[cat_l1_col]),
                'l2': str(row[cat_l2_col]),
                'l3': str(row[cat_l3_col])
            })

        # STEP 6: Process NEW suppliers
        self.emit_log("Step 6: Processing new suppliers...", 'info')

        new_rows = []
        total_new = len(new_suppliers)

        # Track flow statistics
        description_flow_count = 0
        po_line_flow_count = 0

        for idx, norm_name in enumerate(new_suppliers):
            self.emit_progress(0.30 + (idx / max(total_new, 1)) * 0.35,
                            f"Processing new supplier {idx+1}/{total_new}: {norm_name[:30]}...")

            supplier_data = normalized_suppliers[norm_name]

            # Assess PO line quality for this supplier
            passing_items, failing_items, pass_rate = assess_supplier_po_quality(supplier_data['items'])

            # Determine which classification flow to use
            # If >50% fail quality check, use description-based flow
            use_description_flow = pass_rate < 0.5

            if use_description_flow:
                flow_reason = f"PO quality: {pass_rate:.0%} pass rate ({len(passing_items)}/{len(supplier_data['items'])} good lines)"
                self.emit_log(f"📝 {norm_name}: Using DESCRIPTION flow - {flow_reason}", 'info')
                description_flow_count += 1
            else:
                flow_reason = f"PO quality: {pass_rate:.0%} pass rate ({len(passing_items)}/{len(supplier_data['items'])} good lines)"
                self.emit_log(f"📦 {norm_name}: Using PO LINE flow - {flow_reason}", 'info')
                po_line_flow_count += 1

            # Enrich supplier (same for both flows)
            enrichment = enrich_supplier(norm_name, api_key, model, temperature, use_grounding, rate_limiter)

            if use_grounding and enrichment['description'] != 'Not available':
                self.emit_log(f"Found {norm_name} via search", 'grounding')

            # Classification - currently both flows use description-based
            # Phase 3 will implement the PO line flow alternative
            if use_description_flow:
                # ORIGINAL FLOW: Classify by supplier name + description
                classification = classify_supplier(norm_name, enrichment['description'], categories,
                                                api_key, model, temperature, rate_limiter)

                product_tags = generate_supplier_product_tags(norm_name, supplier_data['items'],
                                                            api_key, model, temperature, rate_limiter)

            country_names = []
            country_codes = []
            for country in supplier_data['countries']:
                name, code = normalize_country(country)
                if name != 'Unknown':
                    country_names.append(name)
                    country_codes.append(code)

            new_rows.append({
                'Normalized_Supplier_Name': norm_name,
                'Original_Name_Variants': '; '.join(supplier_data['original_names']),
                'Supplier_Description': enrichment['description'],
                'Employee_Count': enrichment['employee_count'],
                'Revenue': enrichment['revenue'],
                'Year_Established': enrichment['year_established'],
                'Overall_Category': classification['l1'],
                'Category_Code': classification['category_code'],
                'Category_Name': classification['category_name'],
                'Category_Confidence': f"{classification['confidence']:.0%}",
                'L3_Token_Probability': f"{classification.get('l3_token_probability', 0.0):.0%}",
                'Confidence_Reasoning': classification.get('confidence_reasoning', ''),
                'Product_Service_Tags': ', '.join(product_tags) if product_tags else '',
                'Total_PO_Items': len(supplier_data['items']),
                'Ship_To_Countries': ', '.join(country_names) if country_names else 'Unknown',
                'Country_Codes': ', '.join(country_codes) if country_codes else 'XX',
                'Last_Updated': datetime.now().strftime('%Y-%m-%d')
            })

            self.emit_log(f"✓ NEW: {norm_name} -> {classification['l1']} / {classification['category_code']}", 'success')
        else:
            # PO LINE FLOW: Hybrid approach
            # Step 1: Classify each PO line individually
            # Step 2: Consolidate low-confidence items into better-fitting categories

            self.emit_log(f"  Step 1: Classifying {len(passing_items)} PO lines individually for {norm_name}...", 'info')

            # Step 1: Classify each passing PO line individually
            classified_lines = classify_po_lines_individually(
                po_line_descriptions=passing_items,
                supplier_name=norm_name,
                categories=categories,
                api_key=api_key,
                model=model,
                temperature=temperature,
                rate_limiter=rate_limiter,
                log_callback=lambda msg: self.emit_log(msg, 'info')
            )

            # Count initial categories
            initial_categories = set([(l['l1'], l['l2'], l['l3']) for l in classified_lines])
            low_conf_count = sum(1 for l in classified_lines if l['confidence'] < 0.6)
            self.emit_log(f"    Initial: {len(initial_categories)} categories, {low_conf_count} low-confidence items", 'info')

            # Step 2: Consolidate low-confidence classifications
            self.emit_log(f"  Step 2: Consolidating low-confidence classifications...", 'info')

            category_groups = consolidate_classifications(
                classified_lines=classified_lines,
                supplier_name=norm_name,
                api_key=api_key,
                model=model,
                temperature=temperature,
                rate_limiter=rate_limiter,
                confidence_threshold=0.6
            )

            self.emit_log(f"  Final: {len(category_groups)} category groupings for {norm_name}", 'info')

            # Prepare country data (same for all rows of this supplier)
            country_names = []
            country_codes = []
            for country in supplier_data['countries']:
                name, code = normalize_country(country)
                if name != 'Unknown':
                    country_names.append(name)
                    country_codes.append(code)

            # Create one row per category group
            for cat_group in category_groups:
                l1_category = cat_group['l1']
                l2_code = cat_group['l2']
                l3_name = cat_group['l3']
                confidence = cat_group['confidence']
                group_po_lines = cat_group['po_lines']

                # Generate product tags only from this group's PO lines
                group_tags = generate_supplier_product_tags(
                    norm_name,
                    group_po_lines,
                    api_key, model, temperature, rate_limiter
                )

                new_rows.append({
                    'Normalized_Supplier_Name': norm_name,
                    'Original_Name_Variants': '; '.join(supplier_data['original_names']),
                    'Supplier_Description': enrichment['description'],
                    'Employee_Count': enrichment['employee_count'],
                    'Revenue': enrichment['revenue'],
                    'Year_Established': enrichment['year_established'],
                    'Overall_Category': l1_category,
                    'Category_Code': l2_code,
                    'Category_Name': l3_name,
                    'Category_Confidence': f"{confidence:.0%}",
                    'L3_Token_Probability': f"{cat_group.get('l3_token_probability', 0.0):.0%}",
                    'Confidence_Reasoning': cat_group.get('confidence_reasoning', ''),
                    'Product_Service_Tags': ', '.join(group_tags) if group_tags else '',
                    'Total_PO_Items': len(group_po_lines),
                    'Ship_To_Countries': ', '.join(country_names) if country_names else 'Unknown',
                    'Country_Codes': ', '.join(country_codes) if country_codes else 'XX',
                    'Last_Updated': datetime.now().strftime('%Y-%m-%d')
                })

                self.emit_log(f"  ✓ NEW ROW: {norm_name} -> {l1_category} / {l2_code} / {l3_name} ({len(group_po_lines)} items, {confidence:.0%} conf)", 'success')

            self.emit_log(f"✓ NEW: {norm_name} -> {len(category_groups)} category rows created", 'success')

            stats['new_suppliers'] += 1

        # Log flow statistics
        self.emit_log(f"Flow summary: {description_flow_count} suppliers used DESCRIPTION flow, {po_line_flow_count} would use PO LINE flow", 'info')

        # STEP 7: Update EXISTING suppliers
        self.emit_log("Step 7: Updating existing suppliers...", 'info')

        updated_rows = {}
        total_existing = len(existing_to_update)

        for idx, norm_name in enumerate(existing_to_update):
            self.emit_progress(0.65 + (idx / max(total_existing, 1)) * 0.20,
                            f"Updating {idx+1}/{total_existing}: {norm_name[:30]}...")

            supplier_data = normalized_suppliers[norm_name]
            existing_row = existing_suppliers[norm_name.lower()].copy()

            new_tags = generate_supplier_product_tags(norm_name, supplier_data['items'],
                                                    api_key, model, temperature, rate_limiter)

            existing_tags = existing_row.get('Product_Service_Tags', '')
            merged_tags = merge_tags(existing_tags, new_tags)

            new_country_names = []
            new_country_codes = []
            for country in supplier_data['countries']:
                name, code = normalize_country(country)
                if name != 'Unknown':
                    new_country_names.append(name)
                    new_country_codes.append(code)

            merged_countries = merge_locations(existing_row.get('Ship_To_Countries', ''), new_country_names)
            merged_codes = merge_locations(existing_row.get('Country_Codes', ''), new_country_codes)

            existing_row['Product_Service_Tags'] = merged_tags
            existing_row['Ship_To_Countries'] = merged_countries
            existing_row['Country_Codes'] = merged_codes

            try:
                existing_count = int(float(str(existing_row.get('Total_PO_Items', 0) or 0).replace(',', '')))
            except (ValueError, TypeError):
                existing_count = 0
            existing_row['Total_PO_Items'] = existing_count + len(supplier_data['items'])

            existing_row['Last_Updated'] = datetime.now().strftime('%Y-%m-%d')

            existing_variants = str(existing_row.get('Original_Name_Variants', '')).split('; ')
            new_variants = supplier_data['original_names'] - set(existing_variants)
            if new_variants:
                all_variants = set(existing_variants) | supplier_data['original_names']
                existing_row['Original_Name_Variants'] = '; '.join([v for v in all_variants if v])

            updated_rows[norm_name.lower()] = existing_row

            self.emit_log(f"✓ UPDATED: {norm_name} (+{len(new_tags)} tags)", 'update')
            stats['updated_suppliers'] += 1

        # STEP 8: Build final DataFrame
        self.emit_log("Step 8: Building and saving Genpact Supplier Master...", 'info')
        self.emit_progress(0.90, "Saving Genpact Supplier Master...")

        final_rows = []

        for norm_name_lower, row_data in existing_suppliers.items():
            if norm_name_lower not in updated_rows:
                final_rows.append(row_data)
                stats['unchanged_suppliers'] += 1

        for row_data in updated_rows.values():
            final_rows.append(row_data)

        final_rows.extend(new_rows)

        results_df = pd.DataFrame(final_rows)

        desired_columns = [
            'Normalized_Supplier_Name', 'Original_Name_Variants',
            'Supplier_Description', 'Employee_Count', 'Revenue', 'Year_Established',
            'Overall_Category', 'Category_Code', 'Category_Name', 'Category_Confidence', 'L3_Token_Probability', 'Confidence_Reasoning',
            'Product_Service_Tags', 'Total_PO_Items',
            'Ship_To_Countries', 'Country_Codes', 'Last_Updated'
        ]

        for col in desired_columns:
            if col not in results_df.columns:
                results_df[col] = ''

        results_df = results_df[[c for c in desired_columns if c in results_df.columns]]
        results_df = results_df.sort_values('Normalized_Supplier_Name', key=lambda x: x.astype(str)).reset_index(drop=True)

        save_genpact_supplier_master(results_df, genpact_sm_path)
        self.emit_log(f"Saved Genpact Supplier Master to: {genpact_sm_path}", 'success')

        stats['total_in_master'] = len(results_df)

        self.emit_progress(1.0, "Processing complete!")
        self.emit_log(f"Complete! New: {stats['new_suppliers']}, Updated: {stats['updated_suppliers']}, "
                    f"Unchanged: {stats['unchanged_suppliers']}, Total: {stats['total_in_master']}", 'success')

        return results_df, stats

    def on_processing_complete(self, results_df: pd.DataFrame, stats: dict):
        """Handle processing completion"""
        self.processing = False
        self.process_btn.configure(state='normal', text="▶ Start Processing")

        self.results_data = results_df
        self.stats = stats

        # Update results tree
        self.update_results_tree(results_df)

        # Update stats
        self.stat_labels['new'].configure(text=str(stats['new_suppliers']))
        self.stat_labels['updated'].configure(text=str(stats['updated_suppliers']))
        self.stat_labels['unchanged'].configure(text=str(stats['unchanged_suppliers']))
        self.stat_labels['total'].configure(text=str(stats['total_in_master']))

        # Enable export
        self.export_btn.configure(state='normal')

        # Update saved path
        output_dir = os.path.dirname(self.po_file_path)
        genpact_sm_path = os.path.join(output_dir, "GenpactSupplierMaster.csv")
        self.saved_path_label.configure(text=f"✓ Auto-saved to: {genpact_sm_path}")

        self.update_output_status(genpact_sm_path)

        # Switch to results tab
        self.notebook.select(3)

        messagebox.showinfo("Complete",
                        f"Processing complete!\n\n"
                        f"New: {stats['new_suppliers']}\n"
                        f"Updated: {stats['updated_suppliers']}\n"
                        f"Unchanged: {stats['unchanged_suppliers']}\n"
                        f"Total: {stats['total_in_master']}\n\n"
                        f"Saved to: {genpact_sm_path}")

    def on_processing_error(self, error_msg: str):
        """Handle processing error"""
        self.processing = False
        self.process_btn.configure(state='normal', text="▶ Start Processing")
        messagebox.showerror("Error", f"Processing failed:\n{error_msg}")

    def update_results_tree(self, df: pd.DataFrame):
        """Update results treeview"""
        self.results_tree.delete(*self.results_tree.get_children())

        columns = list(df.columns)
        self.results_tree['columns'] = columns

        for col in columns:
            self.results_tree.heading(col, text=col)
            self.results_tree.column(col, width=120, minwidth=80)

        for idx, row in df.iterrows():
            values = [str(v)[:100] if not pd.isna(v) else '' for v in row.values]
            self.results_tree.insert('', tk.END, values=values)

    def export_results(self):
        """Export results to CSV"""
        if self.results_data is None:
            return

        file_path = filedialog.asksaveasfilename(
            title="Save CSV",
            defaultextension=".csv",
            initialfile=f"GenpactSupplierMaster_{datetime.now().strftime('%Y%m%d')}.csv",
            filetypes=[("CSV Files", "*.csv")]
        )

        if file_path:
            try:
                self.results_data.to_csv(file_path, index=False, encoding='utf-8')
                messagebox.showinfo("Success", f"Exported to:\n{file_path}")
            except Exception as e:
                messagebox.showerror("Error", f"Export failed:\n{e}")


###########
# Main Entry Point
###########
def main():
    root = tk.Tk()

    # Set theme
    style = ttk.Style()
    if 'clam' in style.theme_names():
        style.theme_use('clam')

    app = SupplierMasterApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
