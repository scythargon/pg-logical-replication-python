from dataclasses import dataclass
from typing import Any, List, Optional

@dataclass
class Wal2JsonChange:
    kind: str  # 'insert', 'update', 'delete'
    schema: str
    table: str
    columnnames: List[str]
    columntypes: List[str]
    columnvalues: List[Any]
    oldkeys: Optional[dict] = None

@dataclass
class Wal2JsonOutput:
    change: List[Wal2JsonChange]

    @classmethod
    def from_dict(cls, data: dict) -> "Wal2JsonOutput":
        changes = [
            Wal2JsonChange(
                kind=change["kind"],
                schema=change["schema"],
                table=change["table"],
                columnnames=change.get("columnnames", change.get("oldkeys", {}).get("keynames", [])),
                columntypes=change.get("columntypes", change.get("oldkeys", {}).get("keytypes", [])),
                columnvalues=change.get("columnvalues", change.get("oldkeys", {}).get("keyvalues", [])),
                oldkeys=change.get("oldkeys")
            )
            for change in data["change"]
        ]
        return cls(change=changes)
