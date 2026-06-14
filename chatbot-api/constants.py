"""
@file constants.py
@brief Single source of truth for domain constants shared across services.

All location aliases, work-mode keywords, skill aliases, and city name mappings
live HERE. Services import from this file — never define duplicates locally.

Why centralised?
  Previously the same data was copy-pasted in 4-5 places with minor
  inconsistencies (missing aliases, different city name strings).
  One change here now propagates to every service automatically.
"""

# ── Location aliases ───────────────────────────────────────────────────────────
# Canonical key → all accepted user-typed aliases (Vietnamese + English + abbrev).
# Keys ("hcm", "hanoi", "danang", …) are the internal identifiers used throughout
# the codebase.  Add new cities or aliases HERE only.
#
# NOTE on bare short aliases (e.g. "dn", "sg", "hue"):
#   Short ASCII strings risk substring matches inside common words.
#   "hue" omitted → would match English "heuristic", "value", "queue".
#   "bd" omitted → would match many abbreviations.
#   Only add an alias if it is specific enough for the Vietnamese IT-job context.

LOCATION_ALIASES: dict[str, list[str]] = {
    "hcm": [
        "hồ chí minh", "ho chi minh", "hcm", "tp.hcm", "tphcm",
        "sài gòn", "saigon", "sg", "hcmc", "tp hcm",
        "thành phố hồ chí minh",
    ],
    "hanoi": [
        "hà nội", "ha noi", "hanoi", "hn", "thủ đô",
    ],
    "danang": [
        "đà nẵng", "da nang", "danang", "đn", "dn",
    ],
    "binhduong": [
        "bình dương", "binh duong",
    ],
    "haiphong": [
        "hải phòng", "hai phong",
    ],
    "hue": [
        "huế",  # bare "hue" omitted to avoid false positives in English queries
    ],
    "hungyen": [
        "hưng yên", "hung yen",
    ],
}

# Canonical key → Gold layer city name (used in Trino SQL WHERE clauses).
# Must match exact values stored in iceberg.gold.dim_location.city_name.
LOCATION_CITY_NAME: dict[str, str] = {
    "hcm":       "Ho Chi Minh",
    "hanoi":     "Ha Noi",
    "danang":    "Da Nang",
    "binhduong": "Binh Duong",
    "haiphong":  "Hai Phong",
    "hue":       "Hue",
    "hungyen":   "Hung Yen",
}

# Flat reverse map: any alias string → canonical key.
# Derived automatically from LOCATION_ALIASES — do not edit manually.
# Used by query_processor for O(1) alias normalisation.
LOCATION_ALIAS_TO_KEY: dict[str, str] = {
    alias: key
    for key, aliases in LOCATION_ALIASES.items()
    for alias in aliases
}

# Flat map: any alias → display city name (for query expansion / search variants).
# e.g. "hcm" → "Ho Chi Minh", "sài gòn" → "Ho Chi Minh"
LOCATION_ALIAS_TO_NAME: dict[str, str] = {
    alias: LOCATION_CITY_NAME[key]
    for key, aliases in LOCATION_ALIASES.items()
    for alias in aliases
}

# ── Work mode keywords ─────────────────────────────────────────────────────────
# Mode key → all trigger phrases (Vietnamese + English).
# Used for detecting and filtering by work mode in user queries and job text.

WORK_MODE_KEYWORDS: dict[str, list[str]] = {
    "remote": [
        "remote", "từ xa", "làm tại nhà", "làm việc tại nhà",
        "work from home", "wfh", "work remotely", "fully remote", "100% remote",
    ],
    "hybrid": [
        "hybrid", "kết hợp", "làm việc kết hợp", "linh hoạt",
    ],
    "onsite": [
        "onsite", "on-site", "office", "at office", "in-office", "in office",
        "tại văn phòng", "làm tại văn phòng", "văn phòng", "làm văn phòng",
        "đi làm trực tiếp",
    ],
}

# ── Skill query aliases ────────────────────────────────────────────────────────
# Maps user-typed skill variants → lowercase canonical DB skill name.
# DB stores skill_name in UPPERCASE; this dict uses LOWERCASE canonicals
# (matching self._skill_map keys in QueryProcessor / self._skill_lookup keys
#  in MarketContextService).
#
# Ground-truth DB canonical names (from dim_skill):
#   GO(453) | KUBERNETES(193) | REACT_NATIVE(205, underscore) | NODE.JS(616)
#   VUE(165) — NOT "VUE.JS" | REACT(914) | NEXT.JS(248) | NUXT.JS(9)
#   ANGULAR | NESTJS(136, no dot) | POSTGRESQL(427) | .NET(742)
#   SCIKITLEARN(9, no hyphen) | CICD(467, no slash) | ELASTICSEARCH(26)
#   ELK STACK(5, separate from ELASTICSEARCH) | MICROSERVICE(380, singular)
#   KAFKA(118) | SPARK(45) | APACHE AIRFLOW(11) | TERRAFORM(90) | C LANGUAGE(75)
#
# Rules:
#   - Omit risky short aliases (js, py, ts, pg, tf, rn) — too many false positives.
#   - Sort by alias length (longest first) when iterating to match "react native"
#     before "react". The _detect_skills / _extract_primary_skill callers already do this.

SKILL_QUERY_ALIASES: dict[str, str] = {
    # ── Golang → GO ────────────────────────────────────────────────────────────
    "golang":         "go",

    # ── Kubernetes abbreviation ────────────────────────────────────────────────
    "k8s":            "kubernetes",

    # ── React Native (DB stores "REACT_NATIVE" with underscore, 205 jobs) ──────
    "react native":   "react_native",
    "react-native":   "react_native",
    "reactnative":    "react_native",   # concatenated (no separator)

    # ── Node.js (DB: "NODE.JS", 616 jobs) ─────────────────────────────────────
    "nodejs":         "node.js",
    "node js":        "node.js",
    "node":           "node.js",   # "node developer" in IT context ≈ Node.js

    # ── Vue (DB: "VUE", 165 jobs — NOT "VUE.JS") ──────────────────────────────
    "vuejs":          "vue",
    "vue.js":         "vue",
    "vue js":         "vue",

    # ── React (DB: "REACT", 914 jobs) ─────────────────────────────────────────
    "reactjs":        "react",
    "react js":       "react",

    # ── Next.js (DB: "NEXT.JS", 248 jobs) ─────────────────────────────────────
    "nextjs":         "next.js",
    "next js":        "next.js",

    # ── Nuxt.js (DB: "NUXT.JS", 9 jobs) ──────────────────────────────────────
    "nuxtjs":         "nuxt.js",
    "nuxt js":        "nuxt.js",

    # ── Angular (DB: "ANGULAR") ────────────────────────────────────────────────
    "angularjs":      "angular",
    "angular js":     "angular",

    # ── NestJS (DB: "NESTJS" — no dot, 136 jobs) ──────────────────────────────
    "nest.js":        "nestjs",
    "nest js":        "nestjs",

    # ── PostgreSQL (DB: "POSTGRESQL", 427 jobs) ────────────────────────────────
    "postgres":       "postgresql",

    # ── .NET (DB: ".NET", 742 jobs) ────────────────────────────────────────────
    "dotnet":         ".net",
    "dot net":        ".net",
    "asp.net":        ".net",
    "aspnet":         ".net",

    # ── Scikit-learn (DB: "SCIKITLEARN", 9 jobs) ──────────────────────────────
    "scikit-learn":   "scikitlearn",
    "scikit learn":   "scikitlearn",
    "sklearn":        "scikitlearn",

    # ── CI/CD (DB: "CICD", no slash, 467 jobs) ────────────────────────────────
    "ci/cd":          "cicd",
    "ci cd":          "cicd",
    "ci-cd":          "cicd",

    # ── Elasticsearch (DB: "ELASTICSEARCH", 26 jobs) ──────────────────────────
    "elastic search": "elasticsearch",

    # ── ELK Stack (DB: "ELK STACK", 5 jobs, separate from ELASTICSEARCH) ──────
    "elk":            "elk stack",

    # ── Microservice (DB: "MICROSERVICE", singular, 380 jobs) ─────────────────
    "microservices":  "microservice",

    # ── Apache tools ──────────────────────────────────────────────────────────
    "apache kafka":   "kafka",           # DB: "KAFKA", 118 jobs
    "apache spark":   "spark",           # DB: "SPARK", 45 jobs
    "airflow":        "apache airflow",  # DB: "APACHE AIRFLOW", 11 jobs

    # ── C Language (DB: "C LANGUAGE", 75 jobs) ────────────────────────────────
    "c programming":  "c language",
    "c lang":         "c language",

    # ── Tailwind (DB: "TAILWIND") ─────────────────────────────────────────────
    "tailwindcss":    "tailwind",   # user types without space; DB has "TAILWIND"

    # ── Power BI (DB: "POWER BI" with space) ──────────────────────────────────
    "powerbi":        "power bi",

    # ── Objective-C (DB: "OBJECTIVE C" with space, not hyphen) ───────────────
    "objective-c":    "objective c",

    # ── .NET variants (DB: ".NET") ─────────────────────────────────────────────
    "net core":       ".net",         # ".NET Core" ≈ .NET in job postings
    ".net core":      ".net",

    # ── Vue: bare "vue" already matches skill_map directly (VUE exists in DB)
    # ── Angular: bare "angular" already matches skill_map directly
    # ── React: bare "react" already matches skill_map directly
    # ── Docker/Kubernetes: already in DB with those exact names, no alias needed
    # ── Spring Boot: "SPRING BOOT" does NOT exist in dim_skill (only text content)
    #    → slow-path LLM handles "spring boot" queries via semantic search
}
