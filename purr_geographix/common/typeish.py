from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional


@dataclass
class SQLAnywhereConn:
    astart: str
    dbf: str
    dbn: str
    driver: str
    host: str
    pwd: str
    server: str
    uid: str
    port: Optional[int] = None

    def to_dict(self):
        return asdict(self)
