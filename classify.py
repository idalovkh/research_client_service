"""Classify a job title into frontline / adjacent / excluded.

Classification is title-only. Source tags (e.g. RemoteOK's "customer support"
tag on a gardening job) are not trusted at all — see the project notes for why.

Order matters: excluded is checked first, since a title like
"Customer Success Manager" would otherwise also match frontline-ish words.
"""
import re

EXCLUDED_RE = re.compile(
    r"\b(manager|director|head of|vp|lead|principal|customer success"
    r"|account executive|account manager|sales)\b",
    re.IGNORECASE,
)

# Counts as frontline only if a support-ish phrase is paired with one of the
# frontline role words, OR one of the standalone phrases (help desk, tier 1,
# customer happiness ...) matched on its own. The role/phrase list is drawn
# from real title variants seen across verticals — insurance (claims),
# healthcare/medbilling (patient service), banking (member service), retail
# (guest service) — that a narrower "customer support/service" match alone
# was silently dropping.
ROLE = r"(?:representative|agent|associate|specialist|advisor|advocate|operator|officer)"
# UK/AU/CA postings spell it "centre" — both spellings feed heavily off
# Adzuna's gb/au/ca results, so missing one silently drops a whole region.
CENTRE = r"cent(?:er|re)"

FRONTLINE_RE = re.compile(
    rf"(?:customer (?:support|services?|care|experience|solutions)\s*{ROLE}"
    rf"|client (?:services?|support)\s*{ROLE}"
    rf"|patient services? {ROLE}"
    rf"|claims (?:{ROLE}|support specialist)"
    rf"|member services? {ROLE}"
    rf"|guest services? {ROLE}"
    rf"|billing support {ROLE}"
    rf"|order support {ROLE}"
    rf"|call {CENTRE} {ROLE}"
    rf"|contact {CENTRE} {ROLE}"
    rf"|live chat {ROLE}"
    rf"|chat support {ROLE}"
    rf"|customer happiness (?:specialist|associate|hero)"
    rf"|support (?:hero|ninja)"
    rf"|customer champion"
    rf"|help desk|tier\s*1)",
    re.IGNORECASE,
)

ADJACENT_RE = re.compile(
    r"\b(support engineer|technical support engineer)\b",
    re.IGNORECASE,
)


def classify(title: str) -> str:
    """Return 'frontline', 'adjacent', 'excluded', or 'drop'."""
    if not title:
        return "drop"
    if EXCLUDED_RE.search(title):
        return "excluded"
    if FRONTLINE_RE.search(title):
        return "frontline"
    if ADJACENT_RE.search(title):
        return "adjacent"
    return "drop"


# No API here exposes a real "remote" flag, so this is a heuristic over the
# free-text location and title strings ("Remote", "Remote - India", "Anywhere",
# "WFH"). Adzuna in particular tends to give a real city as location even for
# jobs whose *title* says "Remote" (the title is often the only place it shows
# up for that source) — so both fields are checked. This still misses jobs
# where remote-ness is only mentioned in the description, and it will not
# catch on-site jobs that happen to say "Remote Team" in some unrelated sense.
REMOTE_RE = re.compile(
    r"\b(remote|anywhere|work from home|wfh|distributed|telecommute)\b",
    re.IGNORECASE,
)


def is_remote(location: str, title: str = "") -> bool:
    if location and REMOTE_RE.search(location):
        return True
    if title and REMOTE_RE.search(title):
        return True
    return False


# Channel is an additive tag, not a narrower bucket: most titles don't say
# "phone" or "chat" at all, so treating "unspecified" as a hard drop would
# hide the bulk of the real market. classify() decides *whether* a job is
# frontline; channel() only labels *which* channel it says, when it says one.
PHONE_RE = re.compile(
    r"\b(call center|call centre|phone support|telephone|inbound|outbound|dialer)\b",
    re.IGNORECASE,
)
CHAT_RE = re.compile(
    r"\b(live chat|chat support|chat agent|chat specialist)\b",
    re.IGNORECASE,
)


def channel(title: str) -> str:
    """Return 'phone', 'chat', or 'unspecified' — heuristic over the title only."""
    if not title:
        return "unspecified"
    if PHONE_RE.search(title):
        return "phone"
    if CHAT_RE.search(title):
        return "chat"
    return "unspecified"


# A posting from a company that ALREADY sells outsourced support is a
# stronger demand signal than a posting from a company hiring in-house:
# growth in a BPO vendor's own hiring reflects client contracts it has
# already won, not a hypothetical "might outsource someday". Matched as a
# case-insensitive substring against the company name — these are legal-name
# variants seen on job boards (e.g. "Conduent State & Local Solutions, Inc"),
# not exact matches.
BPO_VENDORS = [
    "concentrix", "ttec", "teleperformance", "foundever", "sitel",
    "alorica", "taskus", "ibex", "vxi", "sutherland", "startek", "iqor",
    "sykes", "resultscx", "conduent", "maximus", "genpact", "wns",
    "infosys bpm", "hgs", "atento", "webhelp", "konecta", "arise",
    "working solutions", "liveops", "qureos", "mci careers", "onemci",
    # Pulled from Wikipedia's "Business process outsourcing companies"
    # category (global + United States) — see sources.fetch_wikipedia_bpo_candidates.
    # Generic/ambiguous names from that list (e.g. "Hugo Inc.", bare "NCS",
    # "CSC") were left out as too collision-prone for substring matching.
    "247.ai", "accenture", "capgemini", "cds global", "cognizant",
    "computershare", "convergys", "equifax workforce solutions",
    "hcltech", "hewitt associates", "minacs", "moltiply", "ncs group",
    "outreach calling", "pactera", "stream global services", "tdcx",
    "telus digital", "telus international", "transcom", "unisys",
    "vads berhad", "affiliated computer services", "alight solutions",
    "aon hewitt", "firstsource", "group o", "resource pro",
    # Second pass via `python3 jobs.py --check-bpo-candidates`. Skipped:
    # bare acronyms ("ANSR"), HR/IT-staffing consultancies (not call-center
    # BPO), defunct/merged entities, and single generic words ("Vertex") too
    # collision-prone for substring matching.
    "accountor", "advanced contact solutions", "broadridge financial solutions",
    "epldt ventus", "etelecare", "intelenet", "mphasis", "wipro",
    # Found directly while wiring up Breezy HR / Careerjet — these showed up
    # posting exactly the frontline roles we're tracking (chat/voice/email
    # support agents across multiple remote locations), which is itself the
    # BPO/staffing-agency signature.
    "sourcefit", "helpware", "weassist", "there is talent",
    "customer contact services",
]


def is_bpo_vendor(company: str) -> bool:
    if not company:
        return False
    company_lower = company.lower()
    return any(vendor in company_lower for vendor in BPO_VENDORS)
